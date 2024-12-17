fs = require "fs"
path = require "path"
cluster = require('cluster')
assert = require "assert"
{isThisLeader} = require "./leader_election"
CLUSTER_WORKER_ID = if cluster.isMaster then "nocluster" else cluster.worker.id
debuglog = require("debug")("chcargo:cargo@#{CLUSTER_WORKER_ID}")
{Readable, Transform} = require('stream')
merge2 = require('merge2')
split2 = require('split2')

# Use fs.promises API for async file operations
fsAsync = fs.promises

FILENAME_PREFIX = "cargo_"

EXTNAME_UNCOMMITTED = ".uncommitted"

MAX_COMMIT_PER_EXAM_ROUTINE = 1

MIN_TIME = 1000

MIN_ROWS = 100

DEFAULT_COMMIT_INTERVAL = 5000

DEFAULT_MAX_INSET_PARTS = 100

StaticCountWithinProcess = 0

CLICKHOUSE_SETTINGS = wait_end_of_query: 1

CLICKHOUSE_INPUT_FORMAT = 'JSONCompactEachRow'


class Cargo
  toString : -> "[Cargo #{@tableName}]"

  # @param SQLInsertString tableName - Name of the ClickHouse table to insert into
  # @param Object options - Configuration options
  #   @param {String} options.pathToCargoFolder - Path to folder for storing cargo files
  #   @param {Number} [options.maxTime=1000] - Max time in ms before flushing cached rows
  #   @param {Number} [options.maxRows=100] - Max number of rows to cache before flushing
  #   @param {Number} [options.commitInterval=5000] - Interval in ms between commits to ClickHouse
  #   @param {Number} [options.maxInsetParts=100] - Max number of files to combine in one insert
  # @param {Object} [clickhouseClient] - Clickhouse client instance
  constructor: (@tableName, options = {}, clickhouseClient) ->

    @maxTime = Math.max(parseInt(options.maxTime) or MIN_TIME, MIN_TIME)
    @maxRows = Math.max(parseInt(options.maxRows) or MIN_ROWS, MIN_ROWS)
    @commitInterval = Math.max(parseInt(options.commitInterval) or DEFAULT_COMMIT_INTERVAL, @maxTime)
    @maxInsetParts = Math.max(parseInt(options.maxInsetParts) or DEFAULT_MAX_INSET_PARTS, 1)

    # Configure paths and HTTP options
    assert options.pathToCargoFolder, "missing required option: pathToCargoFolder"
    @pathToCargoFolder = options.pathToCargoFolder
    @pathToCargoFile = path.join(@pathToCargoFolder, FILENAME_PREFIX + @tableName)

    # TODO: Consider adding retry mechanism for failed inserts
    @clickhouseClient = clickhouseClient

    debuglog "[new Cargo] @tableName:#{@tableName}, @maxTime:#{@maxTime}, @maxRows:#{@maxRows}, @commitInterval:#{@commitInterval}, @pathToCargoFile:#{@pathToCargoFile}"

    # Initialize state
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
    debuglog("#{@} [flushToFile] @_isFlushing:", @_isFlushing)
    return if @_isFlushing

    unless @cachedRows.length > 0
      #debuglog("#{@} [flushToFile] nothing to flush")
      @lastFlushAt = Date.now()
      return

    unless forced or (@cachedRows.length > @maxRows) or (Date.now() > @lastFlushAt + @maxTime)
      debuglog("#{@} [flushToFile] SKIP threshold not reach")
      return

    @_isFlushing = true

    rowsToFlush = @cachedRows
    @cachedRows = []
    debuglog("#{@} [flushToFile] -> #{rowsToFlush.length} rows")

    try
      await fsAsync.appendFile(@pathToCargoFile, rowsToFlush.join("\n")+"\n")
    catch err
      debuglog "#{@} [flushToFile] #{rowsToFlush.length} rows FAILED error:", err
      @cachedRows = rowsToFlush.concat(@cachedRows) # unshift data back

    debuglog "#{@} [flushToFile] SUCCESS #{rowsToFlush.length} rows"
    @lastFlushAt = Date.now()
    @_isFlushing = false
    return

  # flush memory cache to the disk file
  flushSync : ->
    unless @cachedRows.length > 0
      debuglog "#{@} [flushSync] nothing to flush"
      return

    rowsToFlush = @cachedRows
    @cachedRows = []
    debuglog("#{@} [flushSync] #{rowsToFlush.length} rows")
    fs.appendFileSync(@pathToCargoFile, rowsToFlush.join("\n")+"\n")
    return

  # upload a single cargo file to clickhouse
  uploadCargoFile : (filepath)->
    debuglog "[uploadCargoFile #{@tableName}] filepath:", filepath

    return await @clickhouseClient.insert({
      table: @tableName,
      format: CLICKHOUSE_INPUT_FORMAT
      values: fs.createReadStream(filepath)
      clickhouse_settings : CLICKHOUSE_SETTINGS
    })

  # upload multiple local uncommits to clickhouse
  uploadLocalUncommitsToClickhouse : (filenamList)->
    debuglog "[uploadLocalUncommitsToClickhouse] filenamList #{filenamList.length} :", filenamList
    return unless Array.isArray(filenamList) and filenamList.length > 0

    listOfFileStreams = filenamList.map (filepath)->
      fs.createReadStream(filepath, {encoding: 'utf8'}).pipe(split2(JSON.parse))

    combinedStream = merge2(listOfFileStreams..., {objectMode: true})
    debuglog "[uploadLocalUncommitsToClickhouse] combinedStream  #{listOfFileStreams.length}"

    await @clickhouseClient.insert({
      table: @tableName,
      format: CLICKHOUSE_INPUT_FORMAT
      values: combinedStream
      clickhouse_settings : CLICKHOUSE_SETTINGS
    })

    for filepath in filenamList
      debuglog "[uploadLocalUncommitsToClickhouse] clean local commit:", path.basename(filepath)
      await fsAsync.unlink(filepath)  # remove successfully committed local files

    return

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
      #debuglog "[commitToClickhouseDB] CANCLE empty filenamList"
      #@_isCommiting = false  # lock release
      return

    # filter out non-commits
    rotationPrefix = FILENAME_PREFIX + @tableName + '.'
    filenamList = filenamList.filter (item)->
      return item.startsWith(rotationPrefix) and item.endsWith(EXTNAME_UNCOMMITTED)

    unless filenamList.length > 0
      #debuglog "[commitToClickhouseDB > ls] CANCLE empty valid filenamList"
      #@_isCommiting = false  # lock release
      return

    debuglog "[commitToClickhouseDB] filenamList(#{filenamList.length})" #, filenamList

    # sort filenamList from new to old
    splitMark = "#{FILENAME_PREFIX}#{@tableName}."
    filenamList.sort (a, b)->
      tsA = parseInt((a.split(splitMark) || [])[1] || 0, 36)
      tsB = parseInt((b.split(splitMark) || [])[1] || 0, 36)
      #debuglog "[commitToClickhouseDB] tsA:#{tsA} tsB:#{tsB}"
      return tsB - tsA
    #debuglog "[commitToClickhouseDB] AFTER SORT filenamList(#{filenamList.length})" #, filenamList

    # truncate filenamList if too long
    filenamList.length = Math.min(filenamList.length,  @maxInsetParts)
    #debuglog "[commitToClickhouseDB] after truncate filenamList(#{filenamList.length})" #, filenamList

    # padding full file path
    filenamList = filenamList.map (item)=> path.join(@pathToCargoFolder, item)

    # submit each local uncommit sequentially
    @uploadLocalUncommitsToClickhouse(filenamList)

    #@_isCommiting = false  # lock release
    return

module.exports = Cargo
