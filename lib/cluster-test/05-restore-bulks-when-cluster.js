const { createCargo, isInited, getClickhouseClient } = require("../");
const debuglog = require("debug")("chcargo:cluster-test:05");
const assert = require("assert");
const fs = require("fs");
const os = require("os");
const path = require("path");
const cluster = require('cluster');
const { rimrafSync } = require('rimraf');
const { mkdirp } = require('mkdirp');

const TABLE_NAME = "default.unittest05";

let STATEMENT_CREATE_TABLE = `CREATE TABLE ${TABLE_NAME}
(
  \`time\` DateTime ,
  \`step\`  UInt32,
  \`pos_id\` String DEFAULT ''
)
ENGINE = Memory()`;

STATEMENT_CREATE_TABLE = STATEMENT_CREATE_TABLE.replace(/\n|\r/g, ' ');

const STATEMENT_DROP_TABLE = `DROP TABLE IF EXISTS ${TABLE_NAME}`;
const NUM_OF_LINE = 12;
const folderOfCargoFile = path.join(process.cwd(), "cargo_files");

// clean up local cargo files before test
rimrafSync(folderOfCargoFile);
mkdirp.sync(folderOfCargoFile);

function prepareBulkCachFile(tableName, label) {
  const theFilepath = path.join(
    process.cwd(),
    "cargo_files",
    `cargo_${tableName}.${Date.now().toString(36)}_unittest.nocluster.uncommitted`
  );

  debuglog("[prepare] theFilepath:", theFilepath);

  let content = "";
  for (let i = 0; i < NUM_OF_LINE; i++) {
    const arr = [Math.round(Date.now() / 1000), i, label || "cluster-cargo"];
    content += JSON.stringify(arr) + "\n";
  }
  content = content.substr(0, content.length - 1);
  fs.writeFileSync(theFilepath, content);
}

async function main() {
  if (cluster.isMaster) {
    debuglog("[isMaster] rebuild data table");
    const clickHouseClient = getClickhouseClient();

    await clickHouseClient.exec({
      query: STATEMENT_DROP_TABLE
    });

    await clickHouseClient.exec({
      query: STATEMENT_CREATE_TABLE
    });

    prepareBulkCachFile(TABLE_NAME, "~batchAAA");
    prepareBulkCachFile(TABLE_NAME, "~batchBBB");

    const proc = () => {
      // spawn worker
      for (let i = 0; i < 8; i++) {
        cluster.fork();
      }
    };

    proc();
    setInterval(proc, 20000);

  } else {
    debuglog(`[isWorker ${cluster.worker.id}]`);
    const theCargo = createCargo(TABLE_NAME);
    let StartCountWithinProcess = 0;

    const proc = () => {
      const mark = `worker@${cluster.worker.id}:${StartCountWithinProcess++}`;
      debuglog("proc insert: ", mark);

      for (let i = 0; i < NUM_OF_LINE; i++) {
        theCargo.push(new Date(), i, mark);
      }
    };

    setInterval(proc, 2000);

    const crashProc = () => {
      nonExistingFunc();
    };

    setTimeout(crashProc, 10000 + Math.random() * 60000 >>> 0);
  }
}

main();
