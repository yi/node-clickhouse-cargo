fs = require "fs"
os = require "os"
path = require "path"
debuglog = require("debug")("chcargo:cargo")
Bulk = require "./bulk"

FOLDER_PREFIX = "clichouse-cargo-"



class Cargo
  #toString : -> "[Cargo #{@id}@#{@workingPath}]"
  toString : -> "[Cargo #{@id}]"

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
    debuglog "#{@} [moveToNextBulk]"
    if @curBulk
      @bulks.push(@curBulk)

    @curBulk = new Bulk(@workingPath)
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
        bulk.commit(@clichouseClient, @statement)

    debuglog "#{@} [exam], bulks:", @bulks.length, ", bulksToRemove:", bulksToRemove.length

    for bulk in bulksToRemove
      pos = @bulks.indexOf(bulk)
      debuglog "#{@} [exam] remove bulk: #{bulk.toString()}@#{pos}"
      @bulks.splice(pos, 1) if pos >= 0

    return

  push : ->
    @curBulk.push(Array.from(arguments))
    return ++@count

  getRetiredBulks : -> return @bulks.concat()


module.exports = Cargo




