clickhouse_cargo = require "../"
assert = require ("assert")


describe "init clickhouse_cargo", ->

  it "auto init when env set", (done)->
    assert clickhouse_cargo.isInited(), "should auto init when env set"
    done()
    return
  return


