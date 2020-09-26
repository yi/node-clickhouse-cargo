fs = require "fs"
os = require "os"
path = require "path"
crypto = require('crypto')
cluster = require('cluster')
assert = require "assert"
#Bulk = require "./bulk"
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

StaticCountWithProcess = 0

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

    debuglog "[new Cargo] @statement:#{@statement}, @maxTime:#{@maxTime}, @maxRows:#{@maxRows}, @commitInterval:#{@commitInterval}, @pathToCargoFolder:#{@pathToCargoFolder}"

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

    @cachedRows.push(arr)
    @flushToFile() if (@cachedRows.length > @maxRows) or (Date.now() > @lastFlushAt + @maxRows)
    return

  # flush memory cache to the disk file
  flushToFile : (callbak=NOOP)->
    return if @_isFlushing

    unless @cachedRows.length > 0
      debuglog("#{@} [flushToFile] nothint to flush")
      @lastFlushAt = Date.now()
      callbak()
      return

    debuglog("#{@} [flushToFile] #{@cachedRows.length} rows")

    rowsToFlush = @cachedRows
    @cachedRows = []

    @_isFlushing = true
    fs.appendFile @pathToCargoFile, rowsToFlush.join("\n"), (err)=>
      if err?
        debuglog "#{@} [flushToFile] FAILED error:", err
        @cachedRows = rowsToFlush.concat(@cachedRows) # unshift data back

      debuglog "#{@} [flushToFile] SUCCESS"
      @lastFlushAt = Date.now()
      @_isFlushing = false
      callbak(err)
      return
    return

  # check if to commit disk file to clickhouse DB
  exam : ->
    unless Date.now() > @lastCommitAt + @commitInterval
      #debuglog "[exam] skip"
      return

    #debuglog "[exam] go commit"
    @flushToFile (err)=>
      if err?
        debuglog "[exam] ABORT fail to flush. error:", err
        return

      fs.stat @pathToCargoFile, (err, stats)=>
        if err?
          if err.code is 'ENOENT'
            debuglog "[exam] CANCLE nothing to commit"
            @lastCommitAt = Date.now()
          else
            debuglog "[exam] ABORT fail to stats file. error:", err
          return

        unless stats and (stats.size > 0)
          debuglog "[exam] ABORT empty file."
          return

        # rotate disk file
        pathToRenameFile = path.join(@pathToCargoFolder, "#{FILENAME_PREFIX}#{@id}.#{Date.now().toString(36) + "_#{++StaticCountWithinProcess}"}.#{CLUSTER_WORKER_ID}#{EXTNAME_UNCOMMITTED}")
        fs.rename @pathToCargoFile, pathToRenameFile, (err)=>
          if err?
            debuglog "[exam] ABORT fail to rename file to #{pathToRenameFile}. error:", err
            return

          @commitToClickhouseDB()
          return
        return
      return
    return

  # commit local rotated files to remote ClickHouse DB
  commitToClickhouseDB : ->
    unless @_isCommiting
      debuglog "[commitToClickhouseDB] SKIP is committing"
      return

    # detect leader before every commit because worker might die
    detectLeaderWorker (err, leadWorkerId)=>
      if err?
        debuglog "[commitToClickhouseDB > detectLeaderWorker] FAILED error:", err
        return

      # only one process can commit
      unless leadWorkerId is CLUSTER_WORKER_ID
        debuglog "[commitToClickhouseDB] CANCLE leadWorkerId:#{leadWorkerId} unmatch CLUSTER_WORKER_ID:#{CLUSTER_WORKER_ID}"
        return

      fs.readdir @pathToCargoFolder, (err, filenamList)=>
        if err?
          debuglog "[commitToClickhouseDB > ls] FAILED error:", err
          return

        return unless Array.isArray(filenamList)
        filenamList = filenamList.filter (item)->
          return item.startsWith(FILENAME_PREFIX + @id + '.') and item.endsWith(EXTNAME_UNCOMMITTED)

        return unless filenamList.length > 0
        debuglog "[commitToClickhouseDB] filenamList(#{filenamList.length})", filenamList

        filenamList = filenamList.map (item)-> path.join(@pathToCargoFolder, item)

        @_committing = true  #lock

        dbStream = @clichouseClient.query(@statement, {format:'JSONCompactEachRow'})

        dbStream.on 'error', (err)=>
          debuglog "#{@} [commitToClickhouseDB > DB write] FAILED error:", err
          @_committing = false
          return

        dbStream.on 'finish', =>
          debuglog "#{@} [commitToClickhouseDB] success dbStream:finish"
          @_committing = false
          for filepath in filenamList
            fs.unlink(filepath, NOOP)  # remove the physical file
          return

        for filepath in filenamList
          debuglog "[commitToClickhouseDB] commiting:", filepath
          fs.createReadStream(filepath).pipe(dbStream)
        return
      return
    return

module.exports = Cargo




