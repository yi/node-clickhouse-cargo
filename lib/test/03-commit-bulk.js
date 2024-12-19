const { createCargo, isInited, getClickhouseClient } = require("../");
const debuglog = require("debug")("chcargo:test:03");
const { sleep } = require("../utils");
const assert = require("assert");
const fs = require("fs");
const os = require("os");
const path = require("path");
const ClickHouse = require('@apla/clickhouse');
const { rimraf, rimrafSync } = require('rimraf');
const { mkdirp } = require('mkdirp');

const TABLE_NAME = "default.unittest03";

let STATEMENT_CREATE_TABLE = `CREATE TABLE IF NOT EXISTS ${TABLE_NAME}
(
  \`time\` DateTime ,
  \`step\`  UInt32,
  \`pos_id\` String DEFAULT ''
)
ENGINE = Memory()`;

STATEMENT_CREATE_TABLE = STATEMENT_CREATE_TABLE.replace(/\n|\r/g, ' ');

const STATEMENT_DROP_TABLE = `DROP TABLE IF EXISTS ${TABLE_NAME}`;

const columnValueString = Date.now().toString(36);

const STATEMENT_SELECT = `SELECT * FROM ${TABLE_NAME} WHERE pos_id='${columnValueString}' ORDER BY step;`;

const NUM_OF_LINE = 27891; // NOTE: bulk flushs every 100 lines

describe("commit bulk", function() {
  let clickHouseClient;
  let theCargo;

  this.timeout(30000);
  clickHouseClient = getClickhouseClient();
  theCargo = null;

  before(async function() {
    debuglog("[before]");
    // clean up clickhouse table
    const client = getClickhouseClient();
    await client.exec({
      query: STATEMENT_DROP_TABLE
    });
    await client.exec({
      query: STATEMENT_CREATE_TABLE
    });
    theCargo = createCargo(TABLE_NAME);
    const folderOfCargoFile = path.dirname(theCargo.pathToCargoFile);
    rimrafSync(folderOfCargoFile);
    mkdirp.sync(folderOfCargoFile);
  });

  after(function() {
    process.exit(0);
  });

  it("push to cargo", async function() {
    for (let i = 0; i < NUM_OF_LINE; i++) {
      theCargo.push(new Date(), i, columnValueString);
    }
    await sleep(20); // wait file stream flush
  });

  it("bulk should committed", function(done) {
    assert(!fs.existsSync(theCargo.pathToCargoFile), "local file must be cleared");
    done();
  });

  it("records should be written into remote ClickHouse server", async function() {
    const client = getClickhouseClient();
    const resultSet = await client.query({
      query: STATEMENT_SELECT,
      format: 'JSON'
    });
    const result = await resultSet.json();
    const { data } = result;

    debuglog(`[read db] data length:${data.length}`);
    assert(data.length === NUM_OF_LINE, `unmatching row length local:${NUM_OF_LINE}, remote:${result.length}`);

    for (let i = 0; i < NUM_OF_LINE; i++) {
      assert(data[i].step === i, `unmatching field ${JSON.stringify(data[i])} `);
    }
  });
});
