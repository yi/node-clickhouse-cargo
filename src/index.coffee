
debuglog = require("debug")("chcargo:index")
ClickHouse = require('@apla/clickhouse')
assert = require "assert"
path = require "path"
os = require "os"
fs = require "fs"
Cargo = require "./cargo"

ClickHouseClient = null
STATEMENT_TO_CARGO = {}

CargoOptions = {}

# @param Object config:
#                     .pathToCargoFolder
#                     .maxTime
#                     .maxRows
#                     .commitInterval
init = (config)->
  assert ClickHouseClient is null, "ClickHouseClient has already inited"
  assert config and config.host, "missing host in config"

  config = Object.assign({}, config)  # leave input obj unmodified

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

  debuglog "[init] CargoOptions:", CargoOptions

  ClickHouseClient = new ClickHouse(config)
  ClickHouseClient.ping (err)-> throw(err) if err
  return

# Create a cargo instance.
# Cargos are bind to statements. Call create multiple times with the same statement, will result in one shared cargo.
# @param statement String, sql insert statement
createCargo = (statement)->
  debuglog "[createCargo] statement:#{statement}"
  assert  ClickHouseClient, "ClickHouseClient needs to be inited first"
  statement = String(statement || "").trim()
  assert statement, "statement must not be blank"

  assert  statement.toUpperCase().startsWith("INSERT"), "statement must be an insert sql"

  cargo = STATEMENT_TO_CARGO[statement]
  if cargo
    debuglog "[createCargo] reuse cargo:", cargo.toString()
    return cargo

  cargo = new Cargo(ClickHouseClient, statement, CargoOptions)
  STATEMENT_TO_CARGO[statement] = cargo
  debuglog "[createCargo] cargo:", cargo.toString()
  return cargo

examCargos = ->
  #debuglog "[examCargos]"
  for statement, cargo of STATEMENT_TO_CARGO
    cargo.exam()
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
setInterval(examCargos, 1000)

module.exports =
  init : init
  createCargo : createCargo
  isInited : -> return not not ClickHouseClient
  getClickHouseClient : -> return ClickHouseClient


