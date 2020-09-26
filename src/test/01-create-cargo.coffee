{createCargo, isInited, init} = require "../"
assert = require ("assert")


INIT_OPTION =
  host : "localhost"
  maxTime : 2000
  maxRows : 100
  commitInterval : 8000

QUERY = "INSERT INTO test.cargo0 FORMAT JSONCompactEachRow"
QUERY1 = "INSERT INTO test.cargo1 FORMAT JSONCompactEachRow"

describe "init clickhouse_cargo", ->

  cargo0 = null

  #before (done)->
    #init(INIT_OPTION)
    #done()
    #return

  it "auto init when env set", (done)->
    assert isInited(), "should auto init when env set"
    done()
    return

  it "cargo must be created with an insert statement", (done)->
    assert.throws((()->createCargo()), Error, /blank/)
    assert.throws((()->createCargo("select * from dual")), Error, /insert/)

    cargo0 = createCargo(QUERY)
    #console.log cargo0
    assert cargo0
    assert cargo0.id, "bad cargo0.id"
    assert cargo0.maxTime is INIT_OPTION.maxTime, "bad cargo0.maxTime:#{cargo0.maxTime} => #{INIT_OPTION.maxTime}"
    assert cargo0.maxRows is INIT_OPTION.maxRows, "bad cargo0.maxRows"
    assert cargo0.commitInterval is INIT_OPTION.commitInterval, "bad cargo0.commitInterval"

    done()
    return

  it "create multiple cargoes with the same statement should result in one shared cargo", (done)->
    cargo1 = createCargo(QUERY)
    cargo2 = createCargo(QUERY)
    cargo3 = createCargo(QUERY)
    assert cargo0 is cargo1 is cargo2 is cargo3

    cargo4 = createCargo(QUERY1)
    assert cargo4 isnt cargo3

    done()
    return

  return


