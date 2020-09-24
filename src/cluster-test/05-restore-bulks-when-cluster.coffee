{
  createCargo
  isInited
  getClickHouseClient
} = require "../"
debuglog = require("debug")("chcargo:cluster-test:05")
assert = require ("assert")
crypto = require('crypto')
fs = require "fs"
path = require "path"
cluster = require('cluster')
{
  FILENAME_PREFIX
  toSQLDateString
} = require "../bulk"

TABLE_NAME = "cargo_test.unittest05"

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


if cluster.isMaster
  debuglog "[isMaster]"
  clickHouseClient = getClickHouseClient()
  clickHouseClient.query STATEMENT_DROP_TABLE, ->
    clickHouseClient.query STATEMENT_CREATE_TABLE, ->

      # prepar bulk cache file
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
        arr = [toSQLDateString(new Date), i, "cluster-bulk"]
        content += JSON.stringify(arr) + "\n"
      content = content.substr(0, content.length - 1)
      fs.writeFileSync(theFilepath, content)

      # spawn worker
      for i in [0...4]
        cluster.fork()
      return

else
  debuglog "[isWorker #{cluster.worker.id}]"
  createCargo(STATEMENT_INSERT)




