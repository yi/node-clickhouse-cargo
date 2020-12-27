{
  createCargo
  isInited
} = require "../"
debuglog = require("debug")("chcargo:cluster-test:05")
assert = require ("assert")
crypto = require('crypto')
fs = require "fs"
os = require "os"
path = require "path"
cluster = require('cluster')
ClickHouse = require('@apla/clickhouse')

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

STATEMENT_DROP_TABLE = "DROP TABLE IF EXISTS #{TABLE_NAME}"

NUM_OF_LINE = 12

STATEMENT_INSERT2 = "insert into #{TABLE_NAME}"


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


prepareBulkCachFile = (tableName, label)->

  theFilepath = path.join(process.cwd(), "cargo_files", "cargo_#{tableName}.#{Date.now().toString(36)}_unittest.nocluster.uncommitted")
  debuglog "[prepare] theFilepath:", theFilepath

  content = ""
  for i in [0...NUM_OF_LINE]
    arr = [Math.round(Date.now() / 1000), i, label || "cluster-cargo"]
    content += JSON.stringify(arr) + "\n"
  content = content.substr(0, content.length - 1)
  fs.writeFileSync(theFilepath, content)
  return


if cluster.isMaster
  debuglog "[isMaster]"
  clickHouseClient = getClickHouseClient()
  clickHouseClient.query STATEMENT_DROP_TABLE, ->
    clickHouseClient.query STATEMENT_CREATE_TABLE, ->

      prepareBulkCachFile(TABLE_NAME, "~batchAAA")
      prepareBulkCachFile(TABLE_NAME, "~batchBBB")


      proc = ->
        # spawn worker
        for i in [0...8]
          cluster.fork()
        return

      proc()

      setInterval(proc, 20000)
      return

else
  #statment = not(cluster.worker.id % 2) && STATEMENT_INSERT || STATEMENT_INSERT2
  debuglog "[isWorker #{cluster.worker.id}]" # statment:", statment
  #theCargo = createCargo(statment)
  theCargo = createCargo(TABLE_NAME)

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

  setTimeout(crashProc, 10000 + Math.random() * 60000 >>> 0)


