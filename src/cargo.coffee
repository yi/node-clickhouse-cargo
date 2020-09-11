{ nanoid } = require 'nanoid'
fs = require "fs"
os = require "os"
path = require "path"
Bulk = require "./bulk"


FOLDER_PREFIX = "clichouse-cargo-"


class Cargo

  toString : -> "'[Cargo #{@id}@#{@workingPath}]"

  constructor: (clichouseClient, statement, bulkTTL)->
    @id = nanoid()
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

  push : (data)->
    @curBulk.push(data)
    ++@count
    return


module.exports = Cargo




