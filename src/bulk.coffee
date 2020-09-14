cluster = require('cluster')
path = require "path"
fs = require "fs"

debuglog = require("debug")("chcargo:bulk")

StaticCountWithProcess = 0

toSQLDateString = (date)->
  return date.getUTCFullYear() + '-' +
    ('00' + (date.getUTCMonth()+1)).slice(-2) + '-' +
    ('00' + date.getUTCDate()).slice(-2) + ' ' +
    ('00' + date.getUTCHours()).slice(-2) + ':' +
    ('00' + date.getUTCMinutes()).slice(-2) + ':' +
    ('00' + date.getUTCSeconds()).slice(-2)

class Bulk
  toString : -> "[Bulk #{@id}@#{@pathToFile}]"

  constructor: (@clichouseClient, workingPath)->
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
      item[i] = toSQLDateString(item) if item instanceof Date

    line =  JSON.stringify(arr) + "\n"
    debuglog "#{@} [push] line:", line

    @outputStream.write(line)
    return

  # set the expiration of this bulk
  expire : (ttl)->
    debuglog "#{@} [expire] ttl:#{ttl}"
    ttl = parseInt(ttl) || 0
    ttl = 1000 if ttl < 1000
    @expireAt = Date.now() + ttl
    return

  commit : ->
    return if @_committing

    return

  isExpired : -> return (parseInt(@expireAt) || 0) <= Date.now()

  isEmpty : -> return @count is 0

  isCommitted : -> return @_committed is true



module.exports = Bulk

