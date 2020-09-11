{createCargo, isInited} = require "../"
assert = require ("assert")


describe "init clickhouse_cargo", ->

  it "auto init when env set", (done)->
    assert isInited(), "should auto init when env set"
    done()
    return

  it "cargo must be created with an insert statement", (done)->
    assert.throws((()->createCargo()), Error, /blank/)
    assert.throws((()->createCargo("select * from dual")), Error, /insert/)

    query = "INSERT INTO test.cargo0 FORMAT JSONCompactEachRow"
    cargo = createCargo(query)
    assert cargo
    assert cargo.id
    assert cargo.curBulk

    done()
    return

  return


