{createCargo, isInited} = require "../"
assert = require ("assert")

QUERY = "INSERT INTO test.cargo0 FORMAT JSONCompactEachRow"

describe "push log to cargo", ->

  cargo = null

  before (done)->
    cargo = createCargo(QUERY)
    done()
    return

  it "push to cargo", (done)->

    for i in [0...100]
      cargo.push new Date, i, "string"

    done()
    return
  return




