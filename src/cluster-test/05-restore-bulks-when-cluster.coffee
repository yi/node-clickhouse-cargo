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
  toSQLDateString
} = require "../utils"

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

columnValueString = Date.now().toString(36)

STATEMENT_INSERT = "INSERT INTO #{TABLE_NAME}"

STATEMENT_DROP_TABLE = "DROP TABLE IF EXISTS #{TABLE_NAME}"

NUM_OF_LINE = 12

STATEMENT_INSERT2 = "insert into #{TABLE_NAME}"

prepareBulkCachFile = (insertStatement, label)->

  theFilepath = path.join(process.cwd(), "cargo_files", "cargo_#{crypto.createHash('md5').update(insertStatement).digest("hex")}.#{Date.now().toString(36)}_unittest.nocluster.uncommitted")
  debuglog "[prepare] theFilepath:", theFilepath

  content = ""
  for i in [0...NUM_OF_LINE]
    arr = [toSQLDateString(new Date), i, label || "cluster-cargo"]
    content += JSON.stringify(arr) + "\n"
  content = content.substr(0, content.length - 1)
  fs.writeFileSync(theFilepath, content)
  return


if cluster.isMaster
  debuglog "[isMaster]"
  clickHouseClient = getClickHouseClient()
  clickHouseClient.query STATEMENT_DROP_TABLE, ->
    clickHouseClient.query STATEMENT_CREATE_TABLE, ->

      prepareBulkCachFile(STATEMENT_INSERT, "~batchA")
      prepareBulkCachFile(STATEMENT_INSERT2, "~batchB")

      # spawn worker
      for i in [0...8]
        cluster.fork()
      return

else
  statment = not(cluster.worker.id % 2) && STATEMENT_INSERT || STATEMENT_INSERT2
  debuglog "[isWorker #{cluster.worker.id}] statment:", statment
  theCargo = createCargo(statment)

  StartCountWithinProcess = 0

  proc = ->
    mark = "worker@#{cluster.worker.id}:#{StartCountWithinProcess++ }"
    debuglog "proc insert: ", mark
    for i in [0...NUM_OF_LINE]
      theCargo.push new Date, i, mark
    return

  setInterval(proc, 2000)

  crashProc = ->
    nonExistingFunc()
    return

  setTimeout(crashProc, 10000 + Math.random() * 10000 >>> 0)


