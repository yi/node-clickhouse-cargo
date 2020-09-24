fs = require "fs"
os = require "os"
path = require "path"
crypto = require('crypto')
cluster = require('cluster')
assert = require "assert"
debuglog = require("debug")("chcargo:cargo")
Bulk = require "./bulk"
{BonjourElector} = require('followtheleader')

FOLDER_PREFIX = "cargo-"

NOOP = -> return

MAX_COMMIT_PER_EXAM_ROUTINE = 1

class Cargo
  #toString : -> "[Cargo #{@id}@#{@workingPath}]"
  toString : -> "[Cargo #{@id}]"

  constructor: (@clichouseClient, @statement, @bulkTTL, pathToCargoFile, skipRestoration)->
    debuglog "[new Cargo] @statement:#{@statement}, @bulkTTL:#{@bulkTTL}"
    #@id = Date.now().toString(36)
    @id = crypto.createHash('md5').update(@statement).digest("hex")
    @count = 0
    @bulks = []

    #@workingPath = fs.mkdtempSync(path.join(os.tmpdir(), FOLDER_PREFIX))
    @workingPath = path.join(pathToCargoFile, FOLDER_PREFIX + @id)

    if fs.existsSync(@workingPath)
      # directory already exists
      assert fs.statSync(@workingPath).isDirectory(), "#{@workingPath} is not a directory"
      unless skipRestoration
        if cluster.isMaster and Object.keys(cluster.workers).length is 0
          debuglog "[new Cargo] single process, try restoreExistingFiles"
          @restoreExistingFiles()
        else
          # HERE
          debuglog "[new Cargo] cluster worker, to elect lead"
          elector = new BonjourElector(name:@id)
          elector.on 'leader', =>
            debuglog "worker:#{cluster.worker.id} is leader, try restoreExistingFiles"
            return
          elector.on 'error', (err)=>
            debuglog "worker:#{cluster.worker.id} ELECTION error:", err
            return

    else
      # create directory
      fs.mkdirSync(@workingPath)

    @curBulk = null
    @moveToNextBulk()
    return

  setBulkTTL : (val)-> @bulkTTL = val

  restoreExistingFiles : ->
    debuglog "[restoreExistingFiles] @workingPath:", @workingPath
    fs.readdir @workingPath, (err, filenamList)=>
      if err?
        throw err
        return

      return unless Array.isArray(filenamList)
      filenamList = filenamList.filter (item)-> return item.startsWith(Bulk.FILENAME_PREFIX)

      return unless filenamList.length > 0

      debuglog "[restoreExistingFiles] filenamList(#{filenamList.length})" #, filenamList

      for filename in filenamList
        pathToFile = path.join(@workingPath, filename)
        stats = fs.statSync(pathToFile)
        if stats.size <= 0
          debuglog "[restoreExistingFiles] remove empty:#{filename}"
          fs.unlink(pathToFile, NOOP)
        else
          existingBulkId = filename.replace(Bulk.FILENAME_PREFIX, "")
          debuglog "[restoreExistingFiles] restore existing bulk:", existingBulkId
          @bulks.push(new Bulk(@workingPath, existingBulkId))

      return
    return

  moveToNextBulk : ->
    debuglog "#{@} [moveToNextBulk]"
    if @curBulk
      @bulks.push(@curBulk)

    @curBulk = new Bulk(@workingPath)
    @curBulk.expire(@bulkTTL)
    return

  # routine to exame each bulk belongs to this cargo
  exam : ()->
    #debuglog "#{@} [exam]"
    if @curBulk
      if @curBulk.isEmpty()
        # lazy: keep ttl when bulk is empty
        @curBulk.expire(@bulkTTL)
      else if @curBulk.isExpired()
        @moveToNextBulk()

    bulksToRemove = []

    @bulks.sort (a, b)-> return (parseInt(a.id, 36) || 0) - (parseInt(b.id, 36) || 0)

    countIssueCommit = 0

    for bulk in @bulks
      if bulk.isCommitted()
        bulksToRemove.push(bulk)
      else
        if countIssueCommit < MAX_COMMIT_PER_EXAM_ROUTINE
          bulk.commit(@clichouseClient, @statement)
          ++countIssueCommit

    debuglog "#{@} [exam], bulks:", @bulks.length, ", bulksToRemove:", bulksToRemove.map((item)-> item.id)

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




