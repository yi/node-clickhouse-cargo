fs = require "fs"
os = require "os"
path = require "path"
debuglog = require("debug")("chcargo:cargo")
Bulk = require "./bulk"

FOLDER_PREFIX = "clichouse-cargo-"


class Cargo
  toString : -> "[Cargo #{@id}@#{@workingPath}]"

  constructor: (@clichouseClient, @statement, @bulkTTL)->
    debuglog "[new Cargo] @statement:#{@statement}, @bulkTTL:#{@bulkTTL}"
    @id = Date.now().toString(36)
    @count = 0
    @workingPath = fs.mkdtempSync(path.join(os.tmpdir(), FOLDER_PREFIX))
    @curBulk = null
    @bulks = []
    @moveToNextBulk()
    return

  setBulkTTL : (val)-> @bulkTTL = val

  moveToNextBulk : ->
    if @curBulk
      @bulks.push(@curBulk)

    @curBulk = new Bulk(@clichouseClient, @workingPath)
    @curBulk.expire(@bulkTTL)
    return

  # routine to exame each bulk belongs to this cargo
  exam : ()->
    debuglog "#{@} [exam]"

    if @curBulk
      if @curBulk.isEmpty()
        # lazy: keep ttl when bulk is empty
        @curBulk.expire(@bulkTTL)
      else if @curBulk.isExpired()
        @moveToNextBulk()

    bulksToRemove = []
    for bulk in @bulks
      if bulk.isCommitted()
        bulksToRemove.push(bulk)
      else
        bulk.commit()

    for bulk in bulksToRemove
      pos = @bulks.indexOf(bulk)
      @bulks.splice(pos, 1) if pos >= 0

    return

  push : ->
    @curBulk.push(Array.from(arguments))
    return ++@count


module.exports = Cargo




