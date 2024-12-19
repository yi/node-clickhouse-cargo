{
  createCargo
  isInited
  getClickhouseClient
} = require "../"
debuglog = require("debug")("chcargo:test:03")
{sleep} = require "../utils"
assert = require ("assert")
fs = require "fs"
os = require "os"
path = require "path"
ClickHouse = require('@apla/clickhouse')
{ rimraf, rimrafSync} = require('rimraf')
{ mkdirp } = require('mkdirp')

TABLE_NAME = "default.unittest03"

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

STATEMENT_DROP_TABLE = "DROP TABLE IF EXISTS #{TABLE_NAME}"

columnValueString = Date.now().toString(36)

STATEMENT_SELECT = "SELECT * FROM #{TABLE_NAME} WHERE pos_id='#{columnValueString}' ORDER BY step;"

# refer
INIT_OPTION =
  host : "localhost"
  maxTime : 2000
  maxRows : 100
  commitInterval : 8000

NUM_OF_LINE = 27891  # NOTE: bulk flushs every 100 lines


describe "commit bulk", ->
  @timeout(30000)

  clickHouseClient = getClickhouseClient()
  theCargo = null

  before ->
    debuglog "[before]"

    # clean up clickhouse table
    client = getClickhouseClient()
    await client.exec({query: STATEMENT_DROP_TABLE})
    await client.exec({query: STATEMENT_CREATE_TABLE})

    theCargo = createCargo(TABLE_NAME);

    # clean up cargo file
    folderOfCargoFile = path.dirname(theCargo.pathToCargoFile)
    rimrafSync(folderOfCargoFile)
    mkdirp.sync(folderOfCargoFile)
    return

  after -> process.exit(0)

  it "push to cargo", ->
    for i in [0...NUM_OF_LINE]
      theCargo.push(new Date, i, columnValueString)

    await sleep 20 # wait file stream flush
    return

  it "bulk should committed", (done)->
    assert !fs.existsSync(theCargo.pathToCargoFile), "local file must be cleared"
    done()
    return

  it "records should be written into remote ClickHouse server", ->

    client = getClickhouseClient()
    resultSet = await client.query({query: STATEMENT_SELECT, format: 'JSON' })
    result = await resultSet.json()
    {data} = result
    debuglog "[read db] data length:#{data.length}"

    assert data.length is NUM_OF_LINE, "unmatching row length local:#{NUM_OF_LINE}, remote:#{result.length}"
    for i in [0...NUM_OF_LINE]
      assert data[i].step is i, "unmatching field #{JSON.stringify(data[i])} "
    return

  return


