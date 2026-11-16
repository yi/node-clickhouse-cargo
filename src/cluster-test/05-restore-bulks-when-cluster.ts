import { createCargo, isInited, getClickhouseClient } from "../";
import debug from "debug";
import assert from "assert";
import fs from "fs";
import path from "path";
import cluster from 'cluster';
import { rimrafSync } from 'rimraf';
import { mkdirp } from 'mkdirp';

const debuglog = debug("chcargo:cluster-test:05");

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

function prepareBulkCachFile(tableName: string, label?: string): void {
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

async function main(): Promise<void> {
  if (cluster.isPrimary) {
    debuglog("[isPrimary] rebuild data table");
    const clickHouseClient = getClickhouseClient()!;

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
    debuglog(`[isWorker ${cluster.worker!.id}]`);
    const theCargo = createCargo(TABLE_NAME);
    let StartCountWithinProcess = 0;

    const proc = () => {
      const mark = `worker@${cluster.worker!.id}:${StartCountWithinProcess++}`;
      debuglog("proc insert: ", mark);

      for (let i = 0; i < NUM_OF_LINE; i++) {
        theCargo.push(new Date(), i, mark);
      }
    };

    setInterval(proc, 2000);

    const crashProc = () => {
      // @ts-expect-error intentionally crash the worker to test flush-on-crash
      nonExistingFunc();
    };

    setTimeout(crashProc, 10000 + Math.random() * 60000 >>> 0);
  }
}

main();
