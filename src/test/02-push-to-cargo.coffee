{createCargo, isInited} = require "../"
debuglog = require("debug")("chcargo:test:02")
{
  createCargo
  isInited
  getClickhouseClient
} = require "../"
assert = require ("assert")
fs = require "fs"
os = require "os"
path = require "path"
_ = require "lodash"
{sleep} = require "../utils"
{ rimraf, rimrafSync} = require('rimraf')
{ mkdirp } = require('mkdirp')

TABLE_NAME = "default.unittest02" # Changed from cargo_test.unittest02 to use default database

STATEMENT_INSERT = "INSERT INTO #{TABLE_NAME}"

STATEMENT_DROP_TABLE = "DROP TABLE IF EXISTS #{TABLE_NAME}"

STATEMENT_CREATE_TABLE = """
CREATE TABLE #{TABLE_NAME}
(
  `time` DateTime ,
  `step`  UInt32,
  `pos_id` String DEFAULT ''
)
ENGINE = Memory()
"""
STATEMENT_CREATE_TABLE = STATEMENT_CREATE_TABLE.replace(/\n|\r/g, ' ')

NUM_OF_LINE = 429 # NOTE: bulk flushs every 100 lines

before ->
  # clean up clickhouse table
  client = getClickhouseClient()
  await client.exec({query: STATEMENT_DROP_TABLE})
  await client.exec({query: STATEMENT_CREATE_TABLE})

describe "push log to cargo and flush manully", ->
  @timeout(20000)

  theCargo = createCargo(TABLE_NAME)
  debuglog "pathToCargoFile:", theCargo.pathToCargoFile

  # clean up cargo file
  folderOfCargoFile = path.dirname(theCargo.pathToCargoFile)
  rimrafSync(folderOfCargoFile)
  mkdirp.sync(folderOfCargoFile)

  columnValueString = Date.now().toString(36)

  it "push to cargo and flush manully", ->
    for i in [0...NUM_OF_LINE]
      theCargo.push new Date, i, columnValueString
    await theCargo.flushToFile()
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

    # clean up cargo file
    rimrafSync(folderOfCargoFile)
    mkdirp.sync(folderOfCargoFile)

    done()
    return

describe "push log to cargo and flush automatically", ->
  @timeout(20000)

  theCargo = createCargo("#{TABLE_NAME}_set2")

  # clean up cargo file
  folderOfCargoFile = path.dirname(theCargo.pathToCargoFile)
  rimrafSync(folderOfCargoFile)
  mkdirp.sync(folderOfCargoFile)

  columnValueString = Date.now().toString(36)

  it "push to cargo and flush manully", ->
    for i in [0...NUM_OF_LINE]
      theCargo.push new Date, i, columnValueString

    await sleep(5)
    return

  it "cargo should flush to file", ->
    assert fs.existsSync(theCargo.pathToCargoFile), "log file not exist on #{theCargo.pathToCargoFile}"
    return

  it "exam content written on hd file", (done)->
    contentInHD = fs.readFileSync(theCargo.pathToCargoFile, 'utf8')
    #debuglog "[exam hd content] contentInHD:", contentInHD
    contentInHDArr =  _.compact(contentInHD.split(/\r|\n|\r\n/))

    debuglog "[exam hd content] contentInHDArr:", contentInHDArr
    assert contentInHDArr.length is NUM_OF_LINE, "unmatching output length. NUM_OF_LINE:#{NUM_OF_LINE}, contentInHDArr.length:#{contentInHDArr.length}"

    for line, i in contentInHDArr
      line = JSON.parse(line)
      #console.log line
      assert line[1] is i, "unmatching field 1 "
      assert line[2] is columnValueString, "unmatching field 2 "


    # clean up cargo file
    rimrafSync(folderOfCargoFile)
    mkdirp.sync(folderOfCargoFile)

    done()

    process.exit(0)
    return
