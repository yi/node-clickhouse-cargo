cluster = require('cluster')
path = require "path"
fs = require "fs"
assert = require "assert"

debuglog = require("debug")("chcargo:bulk")

StaticCountWithProcess = 0

# cork stream write
NUM_OF_LINES_TO_CORK = 100

noop = -> return

toSQLDateString = (date)->
  #debuglog "[toSQLDateString] date:", date
  return date.getUTCFullYear() + '-' +
    ('00' + (date.getUTCMonth()+1)).slice(-2) + '-' +
    ('00' + date.getUTCDate()).slice(-2) + ' ' +
    ('00' + date.getUTCHours()).slice(-2) + ':' +
    ('00' + date.getUTCMinutes()).slice(-2) + ':' +
    ('00' + date.getUTCSeconds()).slice(-2)

class Bulk
  toString : -> "[Bulk #{@id}@#{@pathToFile}]"

  constructor: (workingPath)->
    @id = Date.now().toString(36) + "_#{++StaticCountWithProcess}"
    # when launch as a worker by pm2
    @id += "_#{cluster.worker.id}" if cluster.isWorker

    @count = 0
    @pathToFile = path.join(workingPath, "bulk-#{@id}")

    @outputStream = fs.createWriteStream(@pathToFile, flags:'a')
    # make sure writableStream is working
    @outputStream.write("")

    @_committed = false
    @_committing = false
    return

  push : (arr)->
    unless Array.isArray(arr) and (arr.length > 0)
      debuglog "#{@} [push] empty arr"
      return

    for item, i in arr
      arr[i] = toSQLDateString(item) if (item instanceof Date)

    line =  JSON.stringify(arr)
    #debuglog "#{@} [push] line:", line

    # the primary intent of writable.cork() is to accommodate a situation in which several small chunks are written to the stream in rapid succession.
    @outputStream.cork() if @count % NUM_OF_LINES_TO_CORK is 0

    @outputStream.write((if @count > 0 then "\n" else "") + line, 'utf8')
    ++@count

    if @count % NUM_OF_LINES_TO_CORK is 0
      process.nextTick(()=> @outputStream.uncork())
    return

  # set the expiration of this bulk
  expire : (ttl)->
    debuglog "#{@} [expire] ttl:#{ttl}"
    ttl = parseInt(ttl) || 0
    ttl = 1000 if ttl < 1000
    @expireAt = Date.now() + ttl
    return

  commit : (clichouseClient, statement)->
    if @_committing
      debuglog "#{@} [commit] IGNORE is _committing"
      return
    assert statement, "missing insert statment"

    @_committing = true  #lock
    debuglog "#{@} [commit] go committing:", statement

    theOutputStream = @outputStream

    theOutputStream.end (err)=>
      if err?
        debuglog "#{@} [commit] FAIL to end stream. error:", err
        @_committing = false  #unlock
        return

      #dbStream = clichouseClient.query statement, (err)=>
      dbStream = clichouseClient.query statement, {format:'JSONCompactEachRow'}, (err)=>
        if err?
          debuglog "#{@} [commit] FAIL db query. error:", err
          @_committing = false  #unlock
          return

        try
          @_committing = false
          @_committed = true
          theOutputStream.destroy()
          readableStream.destroy()
          fs.unlink(@pathToFile, noop)  # remove the physical file
          debuglog "#{@} [commit] success"
        catch err
          debuglog "#{@} [commit] FAILED error:", err


        return

      readableStream = fs.createReadStream(@pathToFile)
      readableStream.pipe(dbStream)
      return
    return

  isExpired : -> return (parseInt(@expireAt) || 0) <= Date.now()

  isEmpty : -> return @count is 0

  isCommitted : -> return @_committed is true



module.exports = Bulk

