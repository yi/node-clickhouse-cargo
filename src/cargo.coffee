fs = require "fs"
os = require "os"
path = require "path"
debuglog = require("debug")("chcargo:cargo")
Bulk = require "./bulk"

FOLDER_PREFIX = "clichouse-cargo-"


class Cargo

  toString : -> "'[Cargo #{@id}@#{@workingPath}]"

  constructor: (clichouseClient, @statement, @bulkTTL)->
    @id = Date.now().toString(36)
    @count = 0
    @workingPath = fs.mkdtempSync(path.join(os.tmpdir(), FOLDER_PREFIX))
    @curBulk = null
    @bulks = []
    @moveToNextBulk()
    return

  moveToNextBulk : ->
    if @curBulk
      @bulks.push(@curBulk)
      @curBulk.upload()

    @curBulk = new Bulk(@workingPath)
    return

  exam : (forceUpload)->
    return

  push : (data)->
    @curBulk.push(data)
    return ++@count


module.exports = Cargo




