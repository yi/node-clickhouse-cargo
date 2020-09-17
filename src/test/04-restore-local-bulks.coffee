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

STATEMENT_DROP_TABLE = "DROP TABLE #{TABLE_NAME}"

STATEMENT_SELECT = "SELECT * FROM #{TABLE_NAME} LIMIT 10000000 FORMAT JSONCompactEachRow "

NUM_OF_LINE = 3396


describe "restore-local-bulks", ->
  @timeout(60000)

  theCargo = null
  theBulk = null
  theFilepath = null

  before (done)->

    getClickHouseClient().query(STATEMENT_CREATE_TABLE, done)
    return

  after (done)->
    debuglog "[after] query:", STATEMENT_DROP_TABLE
    getClickHouseClient().query STATEMENT_DROP_TABLE, (err)->
      done(err)
      process.exit() if not err?
      return
    return

  it "prepare local bulks", (done)->
    PathToCargoFile = path.join(process.cwd(), "cargo_files", "cargo-" + crypto.createHash('md5').update(STATEMENT_INSERT).digest("hex"))

    if fs.existsSync(PathToCargoFile)
      assert fs.statSync(PathToCargoFile).isDirectory(), "#{PathToCargoFile} is not a directory"
    else
      fs.mkdirSync(PathToCargoFile, {recursive:true, mode: 0o755})

    bulkId = Bulk.FILENAME_PREFIX + Date.now().toString(36) + "_1"
    pathToBulk = path.join(PathToCargoFile, bulkId)
    debuglog "[prepare] pathToBulk:", pathToBulk
    outputStream = fs.createWriteStream(pathToBulk)

    for i in NUM_OF_LINE
      arr = [toSQLDateString(new Date), i, "local-bulk"]
      line = JSON.stringify(arr)
      outputStream.write((if i > 0 then "\n" else "") + line, 'utf8')

    theCargo = createCargo(STATEMENT_INSERT)
    theBulk = theCargo.curBulk
    theFilepath = theBulk.pathToFile

    setTimeout(done, 10000)   # wait for cargo.exam
    return

  return




