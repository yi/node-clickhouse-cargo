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
  toString : -> "[Bulk #{@id}]"

  constructor: (workingPath)->
    @id = Date.now().toString(36) + "_#{++StaticCountWithProcess}"
    # when launch as a worker by pm2
    @id += "_#{cluster.worker.id}" if cluster.isWorker

    @count = 0
    @pathToFile = path.join(workingPath, "bulk-#{@id}")

    @outputStream = fs.createWriteStream(@pathToFile, flags:'a')
    # make sure writableStream is working
    @outputStream.write("")
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


module.exports = Bulk

