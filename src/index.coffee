
assert = require "assert"
path = require "path"
os = require "os"
fs = require "fs"
http = require ('http')
https = require('https')
cluster = require('cluster')
{ cargoOptionToHttpOption } = require "./utils"
{eachSeries} = require "async"

CLUSTER_WORKER_ID = if cluster.isMaster then "nocluster" else cluster.worker.id
debuglog = require("debug")("chcargo:index@#{CLUSTER_WORKER_ID}")

Cargo = require "./cargo"

REG_INVALID_SQL_TABLE_NAME_CHAR = /[^\w\d\.\-_]/i
TABLE_NAME_TO_CARGO = {}

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
    #debuglog "[index] res:", res and res.headers and res.headers['x-clickhouse-summary']
    assert res and (res.statusCode is 200), "FAILED to pin ClickHouse server. Server response unexpected status code:#{res and res.statusCode}"
  )).on 'error', (err)->
    debuglog "FAILED to pin ClickHouse server, error:", err
    throw(err)
    return

  if isToFlushBeforeCrash
    # flush in-memroy data when process crash
    process.on 'uncaughtException', (err)->
      debuglog "⚠️⚠️⚠️  [flushSyncInMemoryCargo] ⚠️⚠️⚠️  "
      for tableName, cargo of TABLE_NAME_TO_CARGO
        cargo.flushSync()
      throw err
      return

  return

# Create a cargo instance.
# Cargos are bind to table. Call create multiple times with the same table name, will result in one shared cargo.
# @param tableName String, the name of ClickHouse table which data is inserted
createCargo = (tableName)->
  debuglog "[createCargo] tableName:#{tableName}"
  assert CargoOptions.host and CargoOptions.vehicle, "ClickHouse-Cargo needs to be inited first"
  tableName = String(tableName || "").trim()
  assert tableName and not REG_INVALID_SQL_TABLE_NAME_CHAR.test(tableName), "invalid tableName:#{tableName}"

  cargo = TABLE_NAME_TO_CARGO[tableName]
  if cargo
    debuglog "[createCargo] reuse cargo@#{tableName}:", cargo.toString()
    return cargo

  cargo = new Cargo(tableName, CargoOptions)
  TABLE_NAME_TO_CARGO[tableName] = cargo
  debuglog "[createCargo] cargo@#{tableName}:", cargo.toString()
  return cargo


#examCargos = ->
  ## sleep
  #await new Promise((resolve)=> setTimeout(resolve, 1000))

  #for tableName, cargo of TABLE_NAME_TO_CARGO
    #try
      #startAt = Date.now()
      #await cargo.exam()   # one-by-one
      #debuglog "[examCargos] #{cargo.tableName} takes: #{diff}ms" if (diff = Date.now() - startAt) >  5
    #catch err
      #debuglog "[examCargos] FAILED error:", err

  #await examCargos()
  #return

examOneCargo = (cargo)->
  try
    examStartAt = Date.now()
    await cargo.exam()   # one-by-one
    #debuglog "[examCargos] #{cargo.tableName} takes: #{diff}ms" if (diff = Date.now() - examStartAt) >  5
    debuglog "[examOneCargo] #{cargo.tableName} takes: #{Date.now() - examStartAt}ms"
  catch err
    debuglog "[examOneCargo] #{cargo.tableName} FAILED error:", err
  return

examCargos = ->
  #debuglog "[examCargos]"
  routineStartAt = Date.now()
  await eachSeries(TABLE_NAME_TO_CARGO, examOneCargo)
  msSpent = Date.now() - routineStartAt
  debuglog "[examCargos] rountine takes #{msSpent}ms"
  # sleep till next seconds
  await new Promise((resolve)=> setTimeout(resolve, 1000 - msSpent)) if msSpent < 1000
  setImmediate((-> await examCargos()))
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



