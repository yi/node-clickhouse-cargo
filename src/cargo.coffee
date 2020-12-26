fs = require "fs"
#fsAsync = require('fs/promises')
os = require "os"
path = require "path"
qs   = require ('querystring')
crypto = require('crypto')
cluster = require('cluster')
assert = require "assert"
got = require('got')
#CombinedStream = require('combined-stream')
stream = require('stream')
{promisify} = require('util')
{isThisLeader} = require "./leader_election"
CLUSTER_WORKER_ID = if cluster.isMaster then "nocluster" else cluster.worker.id
debuglog = require("debug")("chcargo:cargo@#{CLUSTER_WORKER_ID}")

pipeline = promisify(stream.pipeline)

# to support node -v < 14
fsAsync =
  rename : promisify(fs.rename)
  unlink : promisify(fs.unlink)
  readdir : promisify(fs.readdir)
  appendFile : promisify(fs.appendFile)
  stat : promisify(fs.stat)

FILENAME_PREFIX = "cargo_"

EXTNAME_UNCOMMITTED = ".uncommitted"

NOOP = -> return

MAX_COMMIT_PER_EXAM_ROUTINE = 1

MIN_TIME = 1000

MIN_ROWS = 100

DEFAULT_COMMIT_INTERVAL = 5000

StaticCountWithinProcess = 0



class Cargo
  toString : -> "[Cargo #{@id}]"

  # @param SQLInsertString statement
  # @param Object options:
  #                     .pathToCargoFolder
  #                     .maxTime
  #                     .maxRows
  #                     .commitInterval
  constructor: (@statement, options={})->
    @maxTime = parseInt(options.maxTime) || MIN_TIME
    @maxTime = MIN_TIME if @maxTime < MIN_TIME

    @maxRows = parseInt(options.maxRows) || MIN_ROWS
    @maxRows = MIN_ROWS if @maxRows < 1

    @commitInterval = parseInt(options.commitInterval) || DEFAULT_COMMIT_INTERVAL
    @commitInterval = DEFAULT_COMMIT_INTERVAL if @commitInterval < @maxTime

    @id = crypto.createHash('md5').update(@statement).digest("hex")
    @count = 0
    @bulks = []

    assert options.pathToCargoFolder, "missing options.pathToCargoFolder"
    @pathToCargoFolder = options.pathToCargoFolder
    @pathToCargoFile = path.join(@pathToCargoFolder, FILENAME_PREFIX + @id)

    debuglog "[new Cargo] @statement:#{@statement}, @maxTime:#{@maxTime}, @maxRows:#{@maxRows}, @commitInterval:#{@commitInterval}, @pathToCargoFile:#{@pathToCargoFile}"

    # verify cargo can write to the destination folder
    fs.access @pathToCargoFolder, fs.constants.W_OK, (err)->
      if err?
        throw new Error "Cargo not able to write to folder #{@pathToCargoFolder}. Due to #{err}"
      return

    @cachedRows = []
    @lastFlushAt = Date.now()
    @lastCommitAt = Date.now()
    return

  # push row insert into memory cache
  push : ->
    arr = Array.from(arguments)
    assert arr.length > 0, "blank row can not be accepted."
    for item, i in arr
      arr[i] = Math.round(item.getDate() / 1000)  if (item instanceof Date)

    @cachedRows.push(JSON.stringify(arr))
    #debuglog "[push] row? #{(@cachedRows.length > @maxRows)}, time? #{(Date.now() > @lastFlushAt + @maxRows)} "
    @flushToFile() if (@cachedRows.length > @maxRows) or (Date.now() > @lastFlushAt + @maxRows)
    return

  # flush memory cache to the disk file
  flushToFile : ->
    #debuglog("#{@} [flushToFile] @_isFlushing:", @_isFlushing)
    return if @_isFlushing

    unless @cachedRows.length > 0
      #debuglog("#{@} [flushToFile] nothing to flush")
      @lastFlushAt = Date.now()
      return

    rowsToFlush = @cachedRows
    @cachedRows = []
    debuglog("#{@} [flushToFile] -> #{rowsToFlush.length} rows")

    @_isFlushing = true
    try
      await fsAsync.appendFile(@pathToCargoFile, rowsToFlush.join("\n")+"\n") (err)=>
    catch err
      debuglog "#{@} [flushToFile] FAILED error:", err
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

  # check if to commit disk file to clickhouse DB
  exam : ->
    if @_isExaming
      debuglog "[exam] SKIP @_isExaming"
      return

    @_isExaming = true  # lock on

    try
      await @flushToFile()
    catch err
      debuglog "[exam] ABORT fail to flush. error:", err
      @_isExaming = false  # release
      return

    unless Date.now() > @lastCommitAt + @commitInterval
      #debuglog "[exam] SKIP tick not reach"
      @_isExaming = false  # release
      return

    unless isThisLeader()
      debuglog "[exam] CANCLE NOT leadWorkerId"
      # non-leader skip 10 commit rounds
      @lastCommitAt = Date.now() + @commitInterval * 10
      @_isExaming = false  # release
      return

    #debuglog "[exam] LEAD COMMIT"

    try
      await @rotateFile()
      await @commitToClickhouseDB()
      @lastCommitAt = Date.now()
    catch err
      debuglog "[exam] FAILED to commit. error:", err

    @_isExaming = false  # release
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
      if err.code is 'ENOENT'
        debuglog "[rotateFile] SKIP nothing to rotate"
      else
        debuglog "[rotateFile] ABORT fail to stats file. error:", err
      @_isFileRotating = false # lock released
      return false

    #debuglog "[rotateFile > stat] err:", err,", stats:", stats
    unless stats and (stats.size > 0)
      debuglog "[rotateFile] SKIP empty file."
      @_isFileRotating = false # lock released
      return false

    # rotate disk file
    pathToRenameFile = path.join(@pathToCargoFolder, "#{FILENAME_PREFIX}#{@id}.#{Date.now().toString(36) + "_#{++StaticCountWithinProcess}"}.#{CLUSTER_WORKER_ID}#{EXTNAME_UNCOMMITTED}")
    debuglog "[rotateFile] rotate to #{pathToRenameFile}"

    try
      await fsAsync.rename(@pathToCargoFile, pathToRenameFile)
    catch err
      debuglog "[rotateFile] ABORT fail to rename file to #{pathToRenameFile}. error:", err

    @_isFileRotating = false
    return true

  # commit local rotated files to remote ClickHouse DB
  commitToClickhouseDB : ->
    if @_isCommiting
      debuglog "[commitToClickhouseDB] SKIP is committing"
      return

    @_isCommiting = true  #lock on

    try
      filenamList = fsAsync.readdir(@pathToCargoFolder)
    catch err
      #debuglog "[commitToClickhouseDB > readdir] err:", err, ", filenamList:", filenamList
      debuglog "[commitToClickhouseDB > ls] FAILED error:", err
      @_isCommiting = false  # lock release
      return

    unless Array.isArray(filenamList) and (filenamList.length > 0)
      @_isCommiting = false  # lock release
      return

    # filter out non-commits
    rotationPrefix = FILENAME_PREFIX + @id + '.'
    filenamList = filenamList.filter (item)->
      return item.startsWith(rotationPrefix) and item.endsWith(EXTNAME_UNCOMMITTED)

    unless filenamList.length > 0
      @_isCommiting = false  # lock release
      return

    debuglog "[commitToClickhouseDB] filenamList(#{filenamList.length})" #, filenamList
    filenamList = filenamList.map (item)=> path.join(@pathToCargoFolder, item)

    # submit each local uncommit sequentially
    for filepath in filenamList
      # submit 1 local uncommit to clickhouse

      httpPostOptions = cargoOptionToHttpOption(
        CargoOptions,
        path: "/?" + qs.stringify({query:@statement, format:'JSONCompactEachRow', 'wait_end_of_query':1 })
      )
      debuglog "[commitToClickhouseDB] submit:#{filepath}, httpPostOptions:", httpPostOptions

      try
        res = await pipeline( fs.createReadStream(filepath), got.stream.post(httpPostOptions))
        debuglog "[commitToClickhouseDB] res:#{res}"
        await fsAsync.unlink(filepath)  # remove successfully commited local file
      catch err
        debuglog "[commitToClickhouseDB] FAIL to commit:#{filepath}, error:", err

    @_isCommiting = false  # lock release
    return

module.exports = Cargo

