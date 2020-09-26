{
  createCargo
  isInited
  getClickHouseClient
} = require "../"
debuglog = require("debug")("chcargo:test:04")
assert = require ("assert")
crypto = require('crypto')
fs = require "fs"
path = require "path"
{
  FILENAME_PREFIX
  toSQLDateString
} = require "../bulk"

TABLE_NAME = "cargo_test.unittest04"

STATEMENT_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS #{TABLE_NAME}
(
  `time` DateTime ,
  `step`  UInt32,
  `pos_id` String DEFAULT ''
)
ENGINE = Memory()
"""
STATEMENT_CREATE_TABLE = STATEMENT_CREATE_TABLE.replace(/\n|\r/g, ' ')

STATEMENT_INSERT = "INSERT INTO #{TABLE_NAME}"

STATEMENT_DROP_TABLE = "DROP TABLE IF EXISTS #{TABLE_NAME}"

STATEMENT_SELECT = "SELECT * FROM #{TABLE_NAME} LIMIT 10000000 FORMAT JSONCompactEachRow "

NUM_OF_LINE = 3396


describe "restore-local-bulks", ->
  @timeout(60000)

  theCargo = null
  theFilepath = null

  before (done)->
    getClickHouseClient().query STATEMENT_DROP_TABLE, (err)->
      throw(err) if err?
      getClickHouseClient().query(STATEMENT_CREATE_TABLE, done)
      return
    return

  it "prepare local bulks", (done)->

    try
      PathToCargoFile = path.join(process.cwd(), "cargo_files", "cargo-" + crypto.createHash('md5').update(STATEMENT_INSERT).digest("hex"))

      if fs.existsSync(PathToCargoFile)
        assert fs.statSync(PathToCargoFile).isDirectory(), "#{PathToCargoFile} is not a directory"
      else
        fs.mkdirSync(PathToCargoFile, {recursive:true, mode: 0o755})

      bulkId = FILENAME_PREFIX + Date.now().toString(36) + "_1"
      theFilepath = path.join(PathToCargoFile, bulkId)
      debuglog "[prepare] theFilepath:", theFilepath

      content = ""
      for i in [0...NUM_OF_LINE]
        arr = [toSQLDateString(new Date), i, "test04"]
        content += JSON.stringify(arr) + "\n"
      content = content.substr(0, content.length - 1)
      fs.writeFileSync(theFilepath, content)

      theCargo = createCargo(STATEMENT_INSERT)

    catch err
      console.log "failed error:", err

    setTimeout(done, 10000)   # wait for cargo.exam
    return


  it "local bulks should be commit to ClickHouse", (done)->
    rows = []
    debuglog "[read db] select:", STATEMENT_SELECT
    getClickHouseClient().query STATEMENT_SELECT, {format:"JSONCompactEachRow"}, (err, result)->
      if err?
        done(err)
        return

      result = result.replace(/\n/g,",").trim().replace(/,$/,'')
      result = "[ #{result} ]"
      #console.dir result
      result = JSON.parse(result)

      debuglog "[read db] result:", result.length
      #console.dir result, depth:10

      assert result.length is NUM_OF_LINE, "unmatching row length local:#{NUM_OF_LINE}, remote:#{result.length}"
      result.sort (a, b)-> return a[1] - b[1]

      for row, i in result
        assert row[1] is i, "unmatching field 1 "
        assert row[2] is "local-bulk" , "unmatching field 2 "

      done()
      return
    return

  it "local bulks should be cleaned", (done)->
    assert fs.existsSync(theFilepath) is false, "local bulks should be cleaned:#{theFilepath}"
    done()
    return

  return




