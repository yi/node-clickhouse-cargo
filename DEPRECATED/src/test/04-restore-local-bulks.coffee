{
  createCargo
  isInited
  getClickhouseClient
} = require "../"
debuglog = require("debug")("chcargo:test:04")
assert = require ("assert")
crypto = require('crypto')
fs = require "fs"
os = require "os"
path = require "path"
{ rimraf, rimrafSync} = require('rimraf')
{ mkdirp } = require('mkdirp')
{sleep} = require "../utils"

TABLE_NAME = "default.unittest04"

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

STATEMENT_SELECT = "SELECT * FROM #{TABLE_NAME} WHERE pos_id='#{columnValueString}' ORDER BY step;"

NUM_OF_LINE = 339
NUM_OF_FILES = 111
TOTAL_ROWS = NUM_OF_LINE * NUM_OF_FILES

describe "restore-local-rotations", ->
  @timeout(200000)

  clickHouseClient = getClickhouseClient()
  theCargo = null
  theFilepath = null

  before ->
    debuglog "[before]"

    # clean up clickhouse table
    client = getClickhouseClient()
    await client.exec({query: STATEMENT_DROP_TABLE})
    await client.exec({query: STATEMENT_CREATE_TABLE})

    theCargo = createCargo(TABLE_NAME)

    # clean up cargo file
    folderOfCargoFile = path.dirname(theCargo.pathToCargoFile)
    rimrafSync(folderOfCargoFile)
    mkdirp.sync(folderOfCargoFile)
    return

  after -> process.exit(0)

  it "prepare local rotations", ->
    for j in [0...NUM_OF_FILES]
      try
        theFilepath = path.join(process.cwd(), "cargo_files", "cargo_#{TABLE_NAME}.#{(Date.now() + j).toString(36)}_unittest.nocluster.uncommitted")
        debuglog "[prepare #{j}] theFilepath:", theFilepath

        content = ""
        for i in [0...NUM_OF_LINE]
          arr = [Math.round((Date.now() + j) / 1000), j * NUM_OF_LINE + i,  columnValueString]
          content += JSON.stringify(arr) + "\n"
        content = content.substr(0, content.length - 1)
        fs.writeFileSync(theFilepath, content)

      catch err
        console.log "failed error:", err


    theCargo = createCargo(TABLE_NAME)
    await sleep 30 # wait file stream flush
    return

  it "local bulks should be commit to ClickHouse", ->
    client = getClickhouseClient()
    resultSet = await client.query({query: STATEMENT_SELECT, format: 'JSON' })
    result = await resultSet.json()
    {data} = result
    debuglog "[read db] data length:#{data.length}"

    assert data.length is TOTAL_ROWS, "unmatching row length local:#{TOTAL_ROWS}, remote:#{result.length}"
    for i in [0...TOTAL_ROWS]
      #debuglog "[read db] i:#{i} data[i]:", data[i]
      assert data[i].step is i, "unmatching field #{JSON.stringify(data[i])} "
    return

  return



