
assert = require "assert"
path = require "path"
os = require "os"
fs = require "fs"
http = require ('http')
https = require('https')
cluster = require('cluster')
{ cargoOptionToHttpOption } = require "./utils"

CLUSTER_WORKER_ID = if cluster.isMaster then "nocluster" else cluster.worker.id
debuglog = require("debug")("chcargo:index@#{CLUSTER_WORKER_ID}")

Cargo = require "./cargo"

STATEMENT_TO_CARGO = {}

CargoOptions = {}

# @param Object config:
#                     .pathToCargoFolder
#                     .maxTime
#                     .maxRows
#                     .commitInterval
init = (config)->
  # config could be simply a string of clickhouse server host
  config = host:config if typeof(config) is "string"

  assert config and config.host, "missing host in config"

  #config = Object.assign({}, config)  # leave input obj unmodified
  CargoOptions.host = config.host
  CargoOptions.port = parseInt(config.port) || 8123
  CargoOptions.user = config.user || "default"
  CargoOptions.password = config.password if typeof(config.password) is "string"
  CargoOptions.vehicle  = if String(config.protocol || '').toLowerCase() is 'https:' then https else http
  CargoOptions.timeout  = config.timeout if config.timeout > 0 and Number.isInteger(config.timeout)

  # prepare disk path
  CargoOptions.pathToCargoFolder = path.resolve(process.cwd(), config.cargoPath || "cargo_files")
  delete config.cargoPath

  # verify cargo can write to the destination folder
  fs.accessSync(CargoOptions.pathToCargoFolder, fs.constants.W_OK) #, "Cargo not able to write to folder #{CargoOptions.pathToCargoFolder}"
  fs.stat CargoOptions.pathToCargoFolder, (err, stats)->
    assert not err?, "Fail to read directory stats. Due to #{err}"
    assert stats.isDirectory(), "Not a directory: #{CargoOptions.pathToCargoFolder}"
    return

  maxTime = parseInt(config.maxTime)
  CargoOptions.maxTime = maxTime if maxTime > 0
  delete config.maxTime

  maxRows = parseInt(config.maxRows)
  CargoOptions.maxRows = maxRows if maxRows > 0
  delete config.maxRows

  commitInterval = parseInt(config.commitInterval)
  CargoOptions.commitInterval = commitInterval if commitInterval > 0
  delete config.commitInterval

  #debuglog "[init] CargoOptions:", CargoOptions

  isToFlushBeforeCrash = config.saveWhenCrash isnt false
  delete config.saveWhenCrash

  # pin the given ClickHouse server
  CargoOptions.vehicle.get(cargoOptionToHttpOption(CargoOptions), ((res)->
    assert res and (res.statusCode is 200), "FAILED to pin ClickHouse server. Server response unexpected status code:#{res and res.statusCode}"
  )).on 'error', (err)->
    debuglog "FAILED to pin ClickHouse server, error:", err
    throw(err)
    return

  if isToFlushBeforeCrash
    # flush in-memroy data when process crash
    process.on 'uncaughtException', (err)->
      debuglog "⚠️⚠️⚠️  [flushSyncInMemoryCargo] ⚠️⚠️⚠️  "
      for statement, cargo of STATEMENT_TO_CARGO
        cargo.flushSync()
      throw err
      return

  return

# Create a cargo instance.
# Cargos are bind to statements. Call create multiple times with the same statement, will result in one shared cargo.
# @param statement String, sql insert statement
createCargo = (statement)->
  debuglog "[createCargo] statement:#{statement}"
  assert CargoOptions.host and CargoOptions.vehicle, "ClickHouse-Cargo needs to be inited first"
  statement = String(statement || "").trim()
  assert statement, "statement must not be blank"

  assert  statement.toUpperCase().startsWith("INSERT"), "statement must be an insert sql"

  cargo = STATEMENT_TO_CARGO[statement]
  if cargo
    debuglog "[createCargo] reuse cargo:", cargo.toString()
    return cargo

  cargo = new Cargo(statement, CargoOptions)
  STATEMENT_TO_CARGO[statement] = cargo
  debuglog "[createCargo] cargo:", cargo.toString()
  return cargo


examCargos = ->
  # sleep
  await new Promise((resolve)=> setTimeout(resolve, 1000))

  debuglog "[examCargos]"
  for statement, cargo of STATEMENT_TO_CARGO
    try
      await cargo.exam()   # one-by-one
    catch err
      debuglog "[examCargos] FAILED error:", err

  await examCargos()
  return

## static init
# NOTE: with env:CLICKHOUSE_CARGO_PROFILE, try init automatically
if process.env.CLICKHOUSE_CARGO_PROFILE
  profileName = process.env.CLICKHOUSE_CARGO_PROFILE
  profileName += ".json" unless path.extname(profileName) is ".json"
  pathToConfig = path.join(os.homedir(), ".clickhouse-cargo", process.env.CLICKHOUSE_CARGO_PROFILE + ".json")
  debuglog "[static init] try auto init from CLICKHOUSE_CARGO_PROFILE"
  try
    profileConfig = JSON.parse(fs.readFileSync(pathToConfig))
  catch err
    debuglog "[static init] FAILED error:", err
  init(profileConfig)

# self examination routine
setTimeout((-> await examCargos()), 100)
# examCargos()

module.exports =
  init : init
  createCargo : createCargo
  isInited : -> return not not (CargoOptions and CargoOptions.host)



