{
  createCargo
  isInited
  getClickHouseClient
} = require "../"
debuglog = require("debug")("chcargo:test:03")
assert = require ("assert")
fs = require "fs"

TABLE_NAME = "cargo_test.unittest03_#{Date.now().toString(36)}"

STATEMENT_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS #{TABLE_NAME}
(
  `time` DateTime ,
  `step`  UInt32,
  `pos_id` String DEFAULT ''
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(time)
ORDER BY (time, step)
TTL time + INTERVAL 1 DAY DELETE
SETTINGS index_granularity = 8192;
"""
STATEMENT_CREATE_TABLE = STATEMENT_CREATE_TABLE.replace(/\n|\r/g, ' ')

STATEMENT_INSERT = "INSERT INTO #{TABLE_NAME}"

STATEMENT_DROP_TABLE = "DROP TABLE #{TABLE_NAME}"

#NUM_OF_LINE = 2789  # NOTE: bulk flushs every 100 lines
NUM_OF_LINE = 9  # NOTE: bulk flushs every 100 lines

describe "commit bulk", ->
  @timeout(60000)

  theCargo = null
  theBulk = null

  before (done)->
    theCargo = createCargo(STATEMENT_INSERT)
    theBulk = theCargo.curBulk

    getClickHouseClient().query(STATEMENT_CREATE_TABLE, done)
    return

  after (done)->
    process.exit()
    done()
    return


  it "push to cargo", (done)->
    for i in [0...NUM_OF_LINE]
      theCargo.push new Date, i, "string"

    setTimeout(done, 10000) # wait file stream flush
    return


  it "bulk should committed", (done)->
    assert theBulk.isCommitted(), "the bulk should committed"

    curBulk = theCargo.curBulk
    assert curBulk isnt theBulk, "previouse bulk should not be the current bulk"
    assert theCargo.getRetiredBulks().length is 0, "committed bulks should be cleared"
    done()
    return

  it "records should be written into remote ClickHouse server", (done)->

    stream = getClickHouseClient().query("SELECT * FROM #{TABLE_NAME} LIMIT 10000000 FORMAT JSONCompactEachRow")
    rows = []
    stream.on 'data', (row)->
      debuglog "[read db] row:", row
      rows.push(JSON.parse(row))
      return

    stream.on 'end', ->
      debuglog "[read db] rows:", rows.length
      assert rows.length is NUM_OF_LINE, "unmatching row length"
      rows.sort (a, b)-> return a[1] - b[1]

      for row in rows
        assert row[1] is i, "unmatching field 1 "
        assert row[2] is "string" , "unmatching field 2 "

      done()
      return
    return
  return



