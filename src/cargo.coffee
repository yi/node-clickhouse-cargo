fs = require "fs"
os = require "os"
path = require "path"
crypto = require('crypto')
cluster = require('cluster')
{ pipeline } = require('stream')
assert = require "assert"
#CombinedStream = require('combined-stream')
MultiStream = require('multistream')
{detectLeaderWorker} = require "./leader_election"
{toSQLDateString} = require "./utils"
CLUSTER_WORKER_ID = if cluster.isMaster then "nocluster" else cluster.worker.id
debuglog = require("debug")("chcargo:cargo@#{CLUSTER_WORKER_ID}")

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

  # @param ClickHouse clichouseClient
  # @param SQLInsertString statement
  # @param Object options:
  #                     .pathToCargoFolder
  #                     .maxTime
  #                     .maxRows
  #                     .commitInterval
  constructor: (@clichouseClient, @statement, options={})->
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
      arr[i] = toSQLDateString(item) if (item instanceof Date)

    @cachedRows.push(JSON.stringify(arr))
    #debuglog "[push] row? #{(@cachedRows.length > @maxRows)}, time? #{(Date.now() > @lastFlushAt + @maxRows)} "
    @flushToFile() if (@cachedRows.length > @maxRows) or (Date.now() > @lastFlushAt + @maxRows)
    return

  # flush memory cache to the disk file
  # @callbak (err, isFlushing:Boolean)
  flushToFile : (callbak=NOOP)->
    #debuglog("#{@} [flushToFile] @_isFlushing:", @_isFlushing)
    if @_isFlushing
      callbak(null, true)
      return

    unless @cachedRows.length > 0
      #debuglog("#{@} [flushToFile] nothing to flush")
      @lastFlushAt = Date.now()
      callbak()
      return

    rowsToFlush = @cachedRows
    @cachedRows = []
    debuglog("#{@} [flushToFile] -> #{rowsToFlush.length} rows")

    @_isFlushing = true
    fs.appendFile @pathToCargoFile, rowsToFlush.join("\n")+"\n", (err)=>
      if err?
        debuglog "#{@} [flushToFile] FAILED error:", err
        @cachedRows = rowsToFlush.concat(@cachedRows) # unshift data back

      debuglog "#{@} [flushToFile] SUCCESS #{rowsToFlush.length} rows"
      @lastFlushAt = Date.now()
      @_isFlushing = false
      callbak(err)
      return
    return

  # check if to commit disk file to clickhouse DB
  exam : ->
    #debuglog "[exam] go commit"
    @flushToFile (err, isFlushing)=>
      if err?
        debuglog "[exam] ABORT fail to flush. error:", err
        return

      if isFlushing
        debuglog "[exam] ABORT isFlushing"
        return

      unless Date.now() > @lastCommitAt + @commitInterval
        #debuglog "[exam] SKIP tick not reach"
        return

      @rotateFile (err)=>
        if err?
          debuglog "[exam > rotateFile] FAILED error:", err
          return
        @commitToClickhouseDB()
        return
      return
    return

  rotateFile : (callbak=NOOP)->
    fs.stat @pathToCargoFile, (err, stats)=>
      #debuglog "[exam > stat] err:", err,", stats:", stats
      if err?
        if err.code is 'ENOENT'
          debuglog "[rotateFile] SKIP nothing to rotate"
          @lastCommitAt = Date.now()
          callbak()
        else
          debuglog "[rotateFile] ABORT fail to stats file. error:", err
          callbak(err)
        return

      unless stats and (stats.size > 0)
        debuglog "[rotateFile] SKIP empty file."
        callbak()
        return

      # rotate disk file
      pathToRenameFile = path.join(@pathToCargoFolder, "#{FILENAME_PREFIX}#{@id}.#{Date.now().toString(36) + "_#{++StaticCountWithinProcess}"}.#{CLUSTER_WORKER_ID}#{EXTNAME_UNCOMMITTED}")
      debuglog "[rotateFile] rotate to #{pathToRenameFile}"
      fs.rename @pathToCargoFile, pathToRenameFile, (err)=>
        if err?
          debuglog "[exam] ABORT fail to rename file to #{pathToRenameFile}. error:", err
          callbak(err)
          return

        callbak()
        #@commitToClickhouseDB()
        return
      return
    return

  # commit local rotated files to remote ClickHouse DB
  commitToClickhouseDB : ->
    if @_isCommiting
      debuglog "[commitToClickhouseDB] SKIP is committing"
      return

    # detect leader before every commit because worker might die
    detectLeaderWorker @id, (err, leadWorkerId)=>
      debuglog "[commitToClickhouseDB > detectLeaderWorker] err:", err, ", leadWorkerId:", leadWorkerId
      if err?
        debuglog "[commitToClickhouseDB > detectLeaderWorker] FAILED error:", err
        return

      # only one process can commit
      unless leadWorkerId is CLUSTER_WORKER_ID
        debuglog "[commitToClickhouseDB] CANCLE leadWorkerId:#{leadWorkerId} unmatch CLUSTER_WORKER_ID:#{CLUSTER_WORKER_ID}"
        return

      fs.readdir @pathToCargoFolder, (err, filenamList)=>
        #debuglog "[commitToClickhouseDB > readdir] err:", err, ", filenamList:", filenamList
        if err?
          debuglog "[commitToClickhouseDB > ls] FAILED error:", err
          return

        return unless Array.isArray(filenamList)
        rotationPrefix = FILENAME_PREFIX + @id + '.'
        filenamList = filenamList.filter (item)->
          return item.startsWith(rotationPrefix) and item.endsWith(EXTNAME_UNCOMMITTED)

        return unless filenamList.length > 0
        debuglog "[commitToClickhouseDB] filenamList(#{filenamList.length})" #, filenamList

        filenamList = filenamList.map (item)=> path.join(@pathToCargoFolder, item)

        @_isCommiting = true  #lock

        dbStream = @clichouseClient.query(@statement, {format:'JSONCompactEachRow'})

        dbStream.on 'error', (err)=>
          debuglog "#{@} [commitToClickhouseDB > DB write] FAILED error:", err
          @_isCommiting = false
          return

        dbStream.on 'finish', =>
          debuglog "#{@} [commitToClickhouseDB] success dbStream:finish"
          @_isCommiting = false
          for filepath in filenamList
            fs.unlink(filepath, NOOP)  # remove the physical file
          return

        combinedStream = new MultiStream( filenamList.map((filepath)->fs.createReadStream(filepath)))
        #console.dir combinedStream , depth:10
        combinedStream.pipe(dbStream)
        return
      return
    return

module.exports = Cargo




