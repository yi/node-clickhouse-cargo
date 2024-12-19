const { createCargo, getClickhouseClient } = require("../");
const debuglog = require("debug")("chcargo:test:04");
const assert = require("assert");
const fs = require("fs");
const os = require("os");
const path = require("path");
const { rimrafSync } = require('rimraf');
const { mkdirp } = require('mkdirp');
const { sleep } = require("../utils");

const TABLE_NAME = "default.unittest04";

let STATEMENT_CREATE_TABLE = `CREATE TABLE IF NOT EXISTS ${TABLE_NAME}
(
  \`time\` DateTime ,
  \`step\`  UInt32,
  \`pos_id\` String DEFAULT ''
)
ENGINE = Memory()`;

STATEMENT_CREATE_TABLE = STATEMENT_CREATE_TABLE.replace(/\n|\r/g, ' ');

const columnValueString = Date.now().toString(36);

const STATEMENT_DROP_TABLE = `DROP TABLE IF EXISTS ${TABLE_NAME}`;

const STATEMENT_SELECT = `SELECT * FROM ${TABLE_NAME} WHERE pos_id='${columnValueString}' ORDER BY step;`;

const NUM_OF_LINE = 339;
const NUM_OF_FILES = 111;
const TOTAL_ROWS = NUM_OF_LINE * NUM_OF_FILES;

describe("restore-local-rotations", function() {
  this.timeout(200000);

  const clickHouseClient = getClickhouseClient();
  let theCargo = null;
  let theFilepath = null;

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
    // clean up cargo file
    const folderOfCargoFile = path.dirname(theCargo.pathToCargoFile);
    rimrafSync(folderOfCargoFile);
    mkdirp.sync(folderOfCargoFile);
  });

  after(function() {
    process.exit(0);
  });

  it("prepare local rotations", async function() {
    for (let j = 0; j < NUM_OF_FILES; j++) {
      try {
        theFilepath = path.join(process.cwd(), "cargo_files", `cargo_${TABLE_NAME}.${(Date.now() + j).toString(36)}_unittest.nocluster.uncommitted`);
        debuglog(`[prepare ${j}] theFilepath:`, theFilepath);

        let content = "";
        for (let i = 0; i < NUM_OF_LINE; i++) {
          const arr = [Math.round((Date.now() + j) / 1000), j * NUM_OF_LINE + i, columnValueString];
          content += JSON.stringify(arr) + "\n";
        }
        content = content.substr(0, content.length - 1);
        fs.writeFileSync(theFilepath, content);
      } catch (err) {
        console.log("failed error:", err);
      }
    }
    theCargo = createCargo(TABLE_NAME);
    await sleep(30); // wait file stream flush
  });

  it("local bulks should be commit to ClickHouse", async function() {
    const client = getClickhouseClient();
    const resultSet = await client.query({
      query: STATEMENT_SELECT,
      format: 'JSON'
    });
    const result = await resultSet.json();
    const { data } = result;

    debuglog(`[read db] data length:${data.length}`);
    assert(data.length === TOTAL_ROWS, `unmatching row length local:${TOTAL_ROWS}, remote:${result.length}`);

    for (let i = 0; i < TOTAL_ROWS; i++) {
      assert(data[i].step === i, `unmatching field ${JSON.stringify(data[i])} `);
    }
  });
});
