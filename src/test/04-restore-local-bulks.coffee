{
  createCargo
  isInited
} = require "../"
debuglog = require("debug")("chcargo:test:04")
assert = require ("assert")
crypto = require('crypto')
fs = require "fs"
os = require "os"
path = require "path"
ClickHouse = require('@apla/clickhouse')

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

columnValueString = Date.now().toString(36)

STATEMENT_DROP_TABLE = "DROP TABLE IF EXISTS #{TABLE_NAME}"

STATEMENT_SELECT = "SELECT * FROM #{TABLE_NAME} WHERE pos_id='#{columnValueString}' LIMIT 100000 FORMAT JSONCompactEachRow "

NUM_OF_LINE = 3396


getClickHouseClient = ()->
  profileName = process.env.CLICKHOUSE_CARGO_PROFILE
  assert profileName, "missing process.env.CLICKHOUSE_CARGO_PROFIL"
  profileName += ".json" unless path.extname(profileName) is ".json"
  pathToConfig = path.join(os.homedir(), ".clickhouse-cargo", profileName )
  debuglog "[getClickHouseClient] try auto init from CLICKHOUSE_CARGO_PROFILE from #{pathToConfig}"
  try
    profileConfig = JSON.parse(fs.readFileSync(pathToConfig))
  catch err
    debuglog "[static init] FAILED error:", err
  return new ClickHouse(profileConfig)


describe "restore-local-rotations", ->
  @timeout(60000)

  clickHouseClient = getClickHouseClient()
  theCargo = null
  theFilepath = null

  before (done)->
    clickHouseClient.query STATEMENT_DROP_TABLE, (err)->
      throw(err) if err?
      clickHouseClient.query(STATEMENT_CREATE_TABLE, done)
      return
    return

  after -> process.exit(0)

  it "prepare local rotations", (done)->

    try
      theFilepath = path.join(process.cwd(), "cargo_files", "cargo_#{TABLE_NAME}.#{Date.now().toString(36)}_unittest.nocluster.uncommitted")
      debuglog "[prepare] theFilepath:", theFilepath

      content = ""
      for i in [0...NUM_OF_LINE]
        arr = [Math.round(Date.now() / 1000), i,  columnValueString]
        content += JSON.stringify(arr) + "\n"
      content = content.substr(0, content.length - 1)
      fs.writeFileSync(theFilepath, content)

      theCargo = createCargo(TABLE_NAME)

    catch err
      console.log "failed error:", err

    setTimeout(done, 20000)   # wait for cargo.exam
    return


  it "local bulks should be commit to ClickHouse", (done)->
    rows = []
    debuglog "[read db] select:", STATEMENT_SELECT
    clickHouseClient.query STATEMENT_SELECT, {format:"JSONCompactEachRow"}, (err, result)->
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
        assert row[2] is  columnValueString, "unmatching field 2 "

      done()
      return
    return

  it "local bulks should be cleaned", (done)->
    assert fs.existsSync(theFilepath) is false, "local bulks should be cleaned:#{theFilepath}"
    done()
    return

  return



