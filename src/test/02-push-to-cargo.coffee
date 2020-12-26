{createCargo, isInited} = require "../"
debuglog = require("debug")("chcargo:test:02")
{
  createCargo
  isInited
} = require "../"
assert = require ("assert")
fs = require "fs"
os = require "os"
path = require "path"
_ = require "lodash"
ClickHouse = require('@apla/clickhouse')

TABLE_NAME = "cargo_test.unittest02"

QUERY = "INSERT INTO #{TABLE_NAME} "

STATEMENT_INSERT = "INSERT INTO #{TABLE_NAME}"

STATEMENT_DROP_TABLE = "DROP TABLE IF EXISTS #{TABLE_NAME}"

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

# refer
INIT_OPTION =
  host : "localhost"
  maxTime : 2000
  maxRows : 100
  commitInterval : 8000

#NUM_OF_LINE = 80
NUM_OF_LINE = 429 # NOTE: bulk flushs every 100 lines

getClickHouseClient = ()->
  profileName = process.env.CLICKHOUSE_CARGO_PROFILE
  assert profileName, "missing process.env.CLICKHOUSE_CARGO_PROFIL"
  profileName += ".json" unless path.extname(profileName) is ".json"
  pathToConfig = path.join(os.homedir(), ".clickhouse-cargo", process.env.profileName + ".json")
  debuglog "[getClickHouseClient] try auto init from CLICKHOUSE_CARGO_PROFILE"
  try
    profileConfig = JSON.parse(fs.readFileSync(pathToConfig))
  catch err
    debuglog "[static init] FAILED error:", err
  return new ClickHouse(profileConfig)

describe "push log to cargo", ->
  @timeout(20000)

  theCargo = null
  columnValueString = Date.now().toString(36)

  before (done)->
    debuglog "[before]"

    getClickHouseClient().query STATEMENT_DROP_TABLE, (err)->
      throw(err) if err?
      getClickHouseClient().query STATEMENT_CREATE_TABLE, (err)->
        throw(err) if err?

        theCargo = createCargo(QUERY)
        fs.unlinkSync(theCargo.pathToCargoFile) if fs.existsSync(theCargo.pathToCargoFile)  # clean up existing log
        done()
        return
      return
    return

  #after -> process.exit(0)

  it "push to cargo", (done)->

    for i in [0...NUM_OF_LINE]
      theCargo.push new Date, i, columnValueString

    setTimeout(done, 5000) # wait file stream flush
    return

  it "cargo should flush to file", ->
    assert fs.existsSync(theCargo.pathToCargoFile), "log file not exist on #{theCargo.pathToCargoFile}"
    return

  it "exam content written on hd file", (done)->
    contentInHD = fs.readFileSync(theCargo.pathToCargoFile, 'utf8')
    #debuglog "[exam hd content] contentInHD:", contentInHD
    contentInHDArr =  _.compact(contentInHD.split(/\r|\n|\r\n/))

    #debuglog "[exam hd content] contentInHDArr:", contentInHDArr
    assert contentInHDArr.length is NUM_OF_LINE, "unmatching output length. NUM_OF_LINE:#{NUM_OF_LINE}, contentInHDArr.length:#{contentInHDArr.length}"

    for line, i in contentInHDArr
      line = JSON.parse(line)
      #console.log line
      assert line[1] is i, "unmatching field 1 "
      assert line[2] is columnValueString, "unmatching field 2 "

    done()
    return


