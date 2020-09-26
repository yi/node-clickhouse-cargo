
debuglog = require("debug")("chcargo:index")
ClickHouse = require('@apla/clickhouse')
assert = require "assert"
path = require "path"
os = require "os"
fs = require "fs"
Cargo = require "./cargo"

ClickHouseClient = null

# for how long (in ms) one bulk should keep accumlate inserts
DEFAULT_BULK_TTL = 5 * 1000
MIN_BULK_TTL = 1000

STATEMENT_TO_CARGO = {}

PathToCargoFile = null


init = (config)->
  assert ClickHouseClient is null, "ClickHouseClient has already inited"
  assert config and config.host, "missing host in config"

  # prepare disk path
  PathToCargoFile = path.resolve(process.cwd(), config.cargoPath || "cargo_files")
  debuglog "[init] PathToCargoFile:", PathToCargoFile
  delete config.cargoPath

  if fs.existsSync(PathToCargoFile)
    assert fs.statSync(PathToCargoFile).isDirectory(), "#{PathToCargoFile} is not a directory"
  else
    fs.mkdirSync(PathToCargoFile, {recursive:true, mode: 0o755})

  ClickHouseClient = new ClickHouse(config)
  ClickHouseClient.ping (err)-> throw(err) if err
  return

# Create a cargo instance.
# Cargos are bind to statements. Call create multiple times with the same statement, will result in one shared cargo.
# @param statement String, sql insert statement
# @param bulkTTL Int, ttl(in ms) for flush accumlated inserts. default: 5000, min: 1000
createCargo = (statement, bulkTTL)->
  debuglog "[createCargo] statement:#{statement}, bulkTTL:#{bulkTTL}"
  assert  ClickHouseClient, "ClickHouseClient needs to be inited first"
  statement = String(statement || "").trim()
  assert statement, "statement must not be blank"

  assert  statement.toUpperCase().startsWith("INSERT"), "statement must be an insert sql"

  bulkTTL = parseInt(bulkTTL) || DEFAULT_BULK_TTL
  bulkTTL = MIN_BULK_TTL if bulkTTL < MIN_BULK_TTL

  cargo = STATEMENT_TO_CARGO[statement]
  if cargo
    cargo.setBulkTTL(bulkTTL)
    debuglog "[createCargo] reuse cargo:", cargo.toString()
    return cargo

  cargo = new Cargo(ClickHouseClient, statement, PathToCargoFile, bulkTTL)
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
setInterval(examCargos, MIN_BULK_TTL)

module.exports =
  init : init
  createCargo : createCargo
  isInited : -> return not not ClickHouseClient
  getClickHouseClient : -> return ClickHouseClient


