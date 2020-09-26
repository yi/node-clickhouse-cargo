{
  createCargo
  isInited
  getClickHouseClient
} = require "../"
debuglog = require("debug")("chcargo:test:03")
assert = require ("assert")
fs = require "fs"

TABLE_NAME = "cargo_test.unittest03"

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

columnValueString = Date.now().toString(36)

#STATEMENT_SELECT = "SELECT * FROM #{TABLE_NAME} LIMIT 10000000"
STATEMENT_SELECT = "SELECT * FROM #{TABLE_NAME} WHERE pos_id='#{columnValueString}' LIMIT 100000 FORMAT JSONCompactEachRow "

# refer
INIT_OPTION =
  host : "localhost"
  maxTime : 2000
  maxRows : 100
  commitInterval : 8000

NUM_OF_LINE = 27891  # NOTE: bulk flushs every 100 lines
#NUM_OF_LINE = 9  # NOTE: bulk flushs every 100 lines

describe "commit bulk", ->
  @timeout(30000)

  theCargo = null

  before (done)->
    debuglog "[before]"

    getClickHouseClient().query STATEMENT_DROP_TABLE, (err)->
      throw(err) if err?
      getClickHouseClient().query STATEMENT_CREATE_TABLE, (err)->
        throw(err) if err?

        theCargo = createCargo(STATEMENT_INSERT)
        fs.unlinkSync(theCargo.pathToCargoFile) if fs.existsSync(theCargo.pathToCargoFile)  # clean up existing log
        done()
        return
      return
    return

  it "push to cargo", (done)->
    for i in [0...NUM_OF_LINE]
      theCargo.push new Date, i, columnValueString

    setTimeout(done, 15000) # wait file stream flush
    return

  it "bulk should committed", (done)->
    assert !fs.existsSync(theCargo.pathToCargoFile), "local file must be cleared"
    done()
    return


  it "records should be written into remote ClickHouse server", (done)->

    rows = []
    debuglog "[read db] select:", STATEMENT_SELECT
    stream = getClickHouseClient().query STATEMENT_SELECT, {format:"JSONCompactEachRow"}, (err, result)->
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
        assert row[2] is columnValueString, "unmatching field 2 "

      done()
      return
    return
  return



