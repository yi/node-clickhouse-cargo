fs = require "fs"
os = require "os"
path = require "path"
qs   = require ('querystring')
cluster = require('cluster')
assert = require "assert"
EventEmitter = require('events')
{promisify} = require('util')
{isThisLeader} = require "./leader_election"
{cargoOptionToHttpOption} = require "./utils"
CLUSTER_WORKER_ID = if cluster.isMaster then "nocluster" else cluster.worker.id
debuglog = require("debug")("chcargo:cargo@#{CLUSTER_WORKER_ID}")
url = require('url')

#pipeline = promisify(stream.pipeline)

# to support node -v < 14
fsAsync =
  rename : promisify(fs.rename)
  unlink : promisify(fs.unlink)
  readdir : promisify(fs.readdir)
  appendFile : promisify(fs.appendFile)
  stat : promisify(fs.stat)

debuglog "[static init] fsAsync:", fsAsync

FILENAME_PREFIX = "cargo_"

EXTNAME_UNCOMMITTED = ".uncommitted"

NOOP = -> return

MAX_COMMIT_PER_EXAM_ROUTINE = 1

MIN_TIME = 1000

MIN_ROWS = 100

DEFAULT_COMMIT_INTERVAL = 5000

StaticCountWithinProcess = 0


class Cargo extends EventEmitter
  toString : -> "[Cargo #{@tableName}]"

  # @param SQLInsertString tableName
  # @param Object options:
  #                     .pathToCargoFolder
  #                     .maxTime
  #                     .maxRows
  #                     .commitInterval
  constructor: (@tableName, options={})->
    super()
    @maxTime = parseInt(options.maxTime) || MIN_TIME
    @maxTime = MIN_TIME if @maxTime < MIN_TIME

    @maxRows = parseInt(options.maxRows) || MIN_ROWS
    @maxRows = MIN_ROWS if @maxRows < 1

    @commitInterval = parseInt(options.commitInterval) || DEFAULT_COMMIT_INTERVAL
    @commitInterval = DEFAULT_COMMIT_INTERVAL if @commitInterval < @maxTime

    #@statement = "INSERT INTO #{@tableName} FORMAT JSONCompactEachRow\n"
    @statement = "INSERT INTO #{@tableName} FORMAT JSONCompactEachRow "

    assert options.pathToCargoFolder, "missing options.pathToCargoFolder"
    @pathToCargoFolder = options.pathToCargoFolder
    @pathToCargoFile = path.join(@pathToCargoFolder, FILENAME_PREFIX + @tableName)
    @httpPostOptions = cargoOptionToHttpOption(options, {path: '/?wait_end_of_query=1', method:'POST'})
    @vehicle = options.vehicle

    debuglog "[new Cargo] @tableName:#{@tableName}, @maxTime:#{@maxTime}, @maxRows:#{@maxRows}, @commitInterval:#{@commitInterval}, @pathToCargoFile:#{@pathToCargoFile}"

    # verify cargo can write to the destination folder
    fs.access @pathToCargoFolder, fs.constants.W_OK, (err)->
      if err?
        throw new Error "Cargo not able to write to folder #{@pathToCargoFolder}. Due to #{err}"
      return

    @cachedRows = []
    @lastFlushAt = Date.now()
    @lastCommitAt = Date.now()
    @countRotation = 0
    return

  # push row insert into memory cache
  push : ->
    arr = Array.from(arguments)
    assert arr.length > 0, "blank row can not be accepted."
    for item, i in arr
      if (item instanceof Date)
        arr[i] = Math.round(item.getTime() / 1000)
      else if typeof b is "boolean"
        arr[i] = Number(b)

    @cachedRows.push(JSON.stringify(arr))
    return

  # flush memory cache to the disk file
  # @param forced Boolean, is force to flush file
  flushToFile : (forced)->
    #debuglog("#{@} [flushToFile] @_isFlushing:", @_isFlushing)
    return if @_isFlushing

    unless @cachedRows.length > 0
      #debuglog("#{@} [flushToFile] nothing to flush")
      @lastFlushAt = Date.now()
      return

    unless forced or (@cachedRows.length > @maxRows) or (Date.now() > @lastFlushAt + @maxTime)
      #debuglog("#{@} [flushToFile] SKIP threshold not reach")
      return

    @_isFlushing = true

    rowsToFlush = @cachedRows
    @cachedRows = []
    #debuglog("#{@} [flushToFile] -> #{rowsToFlush.length} rows")

    try
      await fsAsync.appendFile(@pathToCargoFile, rowsToFlush.join("\n")+"\n")
    catch err
      debuglog "#{@} [flushToFile] #{rowsToFlush.length} rows FAILED error:", err
      @cachedRows = rowsToFlush.concat(@cachedRows) # unshift data back

    debuglog "#{@} [flushToFile] SUCCESS #{rowsToFlush.length} rows"
    @lastFlushAt = Date.now()
    @_isFlushing = false
    return

  flushSync : ->
    unless @cachedRows.length > 0
      debuglog "#{@} [flushSync] nothing to flush"
      return

    rowsToFlush = @cachedRows
    @cachedRows = []
    debuglog("#{@} [flushSync] #{rowsToFlush.length} rows")
    fs.appendFileSync(@pathToCargoFile, rowsToFlush.join("\n")+"\n")
    return

  uploadCargoFile : (filepath)->
    debuglog "[uploadCargoFile #{@tableName}] filepath:", filepath
    proc = (resolve, reject)=>
      try
        srcStream = fs.createReadStream(filepath)
      catch err
        reject(err)
        return

      req = @vehicle.request @httpPostOptions, (res)=>
        unless res and res.statusCode is 200
          reject(new Error("ClickHouse server response statusCode:#{res and res.statusCode}"))
          return

        #debuglog "[uploadCargoFile] res.headers:", res.headers
        resolve(res)
        return

      req.on 'error', (err)->
        debuglog "[uploadCargoFile : on err:]", err
        reject(err)
        return

      req.on 'timeout', ->
        debuglog "[uploadCargoFile : on timeout]"
        req.destroy(new Error('request timeout'))
        return

      req.write(@statement)
      srcStream.pipe(req)
      return

    return new Promise(proc)

  # check if to commit disk file to clickhouse DB
  exam : ->
    try
      await @flushToFile()
    catch err
      debuglog "[exam #{@tableName}] ABORT fail to flush. error:", err
      return

    unless Date.now() > @lastCommitAt + @commitInterval
      #debuglog "[exam #{@tableName}] SKIP tick not reach"
      return

    unless isThisLeader()
      debuglog "[exam #{@tableName}] CANCLE NOT leadWorkerId"
      # non-leader skip 10 commit rounds
      @lastCommitAt = Date.now() + @commitInterval * 10
      return

    #debuglog "[exam] LEAD COMMIT"

    try
      hasRotation = await @rotateFile()
      #if hasRotation or @countRotation < 1
      # LAZY: commit only when: 1. has local rotated uncommits, or 2. first time exame to cargo to restore any previous local uncommits
      @countRotation += Number(hasRotation)
      await @commitToClickhouseDB()
      @lastCommitAt = Date.now()
    catch err
      debuglog "[exam #{@tableName}] FAILED to commit. error:", err
    return

  # prepar all uncommitted local files
  # @return Boolean, true if there are local uncommits exist
  rotateFile : ->
    if @_isFileRotating
      debuglog "[rotateFile] SKIP @_isFileRotating"
      return false

    @_isFileRotating = true  # lock on

    try
      stats = await fsAsync.stat(@pathToCargoFile)
    catch err
      if err.code isnt 'ENOENT'
        debuglog "[rotateFile] ABORT fail to stats file. error:", err
      #else
        #debuglog "[rotateFile] SKIP nothing to rotate"
      @_isFileRotating = false # lock released
      return false

    #debuglog "[rotateFile > stat] err:", err,", stats:", stats
    unless stats and (stats.size > 0)
      debuglog "[rotateFile] SKIP empty file."
      @_isFileRotating = false # lock released
      return false

    # rotate disk file
    pathToRenameFile = path.join(@pathToCargoFolder, "#{FILENAME_PREFIX}#{@tableName}.#{Date.now().toString(36) + "_#{++StaticCountWithinProcess}"}.#{CLUSTER_WORKER_ID}#{EXTNAME_UNCOMMITTED}")
    debuglog "[rotateFile] rotate to #{pathToRenameFile}"

    try
      await fsAsync.rename(@pathToCargoFile, pathToRenameFile)
    catch err
      debuglog "[rotateFile] ABORT fail to rename file to #{pathToRenameFile}. error:", err

    @_isFileRotating = false
    return true

  # commit local rotated files to remote ClickHouse DB
  commitToClickhouseDB : ->
    #debuglog "[commitToClickhouseDB]"
    #if @_isCommiting
      #debuglog "[commitToClickhouseDB] SKIP is committing"
      #return

    #@_isCommiting = true  #lock on

    try
      filenamList = await fsAsync.readdir(@pathToCargoFolder)
    catch err
      #debuglog "[commitToClickhouseDB > readdir] err:", err, ", filenamList:", filenamList
      debuglog "[commitToClickhouseDB > ls] FAILED error:", err
      #@_isCommiting = false  # lock release
      return

    #debuglog "[commitToClickhouseDB > readdir] filenamList:", filenamList

    unless Array.isArray(filenamList) and (filenamList.length > 0)
      debuglog "[commitToClickhouseDB] CANCLE empty filenamList"
      #@_isCommiting = false  # lock release
      return

    # filter out non-commits
    rotationPrefix = FILENAME_PREFIX + @tableName + '.'
    filenamList = filenamList.filter (item)->
      return item.startsWith(rotationPrefix) and item.endsWith(EXTNAME_UNCOMMITTED)

    unless filenamList.length > 0
      debuglog "[commitToClickhouseDB > ls] CANCLE empty valid filenamList"
      #@_isCommiting = false  # lock release
      return

    debuglog "[commitToClickhouseDB] filenamList(#{filenamList.length})" #, filenamList
    filenamList = filenamList.map (item)=> path.join(@pathToCargoFolder, item)

    # submit each local uncommit sequentially
    for filepath in filenamList
      try
        res = await @uploadCargoFile(filepath)
        debuglog "[commitToClickhouseDB] res.headers: #{JSON.stringify(res.headers)}"
        await fsAsync.unlink(filepath)  # remove successfully commited local file
      catch err
        debuglog "[commitToClickhouseDB] FAIL to commit:#{filepath}, error:", err
        err.filepath = filepath
        @emit 'error', err

    #@_isCommiting = false  # lock release
    return

module.exports = Cargo

