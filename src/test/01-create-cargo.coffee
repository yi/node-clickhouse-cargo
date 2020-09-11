{createCargo, isInited} = require "../"
assert = require ("assert")


QUERY = "INSERT INTO test.cargo0 FORMAT JSONCompactEachRow"
QUERY1 = "INSERT INTO test.cargo1 FORMAT JSONCompactEachRow"

describe "init clickhouse_cargo", ->

  cargo0 = null

  it "auto init when env set", (done)->
    assert isInited(), "should auto init when env set"
    done()
    return

  it "cargo must be created with an insert statement", (done)->
    assert.throws((()->createCargo()), Error, /blank/)
    assert.throws((()->createCargo("select * from dual")), Error, /insert/)

    cargo0 = createCargo(QUERY)
    assert cargo0
    assert cargo0.id
    assert cargo0.curBulk

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


