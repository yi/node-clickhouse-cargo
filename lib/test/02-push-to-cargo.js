const { createCargo, isInited, getClickhouseClient } = require('../');
const debuglog = require('debug')('chcargo:test:02');
const assert = require('assert');
const fs = require('fs');
const os = require('os');
const path = require('path');
const _ = require('lodash');
const { sleep } = require('../utils');
const { rimrafSync } = require('rimraf');
const { mkdirp } = require('mkdirp');

const TABLE_NAME = 'default.unittest02'; // Changed from cargo_test.unittest02 to use default database
const STATEMENT_DROP_TABLE = `DROP TABLE IF EXISTS ${TABLE_NAME}`;
let STATEMENT_CREATE_TABLE = `CREATE TABLE ${TABLE_NAME}
(
  \`time\` DateTime ,
  \`step\`  UInt32,
  \`pos_id\` String DEFAULT ''
)
ENGINE = Memory()`;

STATEMENT_CREATE_TABLE = STATEMENT_CREATE_TABLE.replace(/\n|\r/g, ' ');

const NUM_OF_LINE = 429; // NOTE: bulk flushs every 100 lines

before(async () => {
  // clean up clickhouse table
  const client = getClickhouseClient();
  await client.exec({
    query: STATEMENT_DROP_TABLE
  });
  await client.exec({
    query: STATEMENT_CREATE_TABLE
  });
});

after(() => {
  process.exit(0);
});

describe('push log to cargo and flush manully', function() {
  this.timeout(20000);

  const theCargo = createCargo(TABLE_NAME);
  debuglog('pathToCargoFile:', theCargo.pathToCargoFile);

  // clean up cargo file
  const folderOfCargoFile = path.dirname(theCargo.pathToCargoFile);
  rimrafSync(folderOfCargoFile);
  mkdirp.sync(folderOfCargoFile);

  const columnValueString = Date.now().toString(36);

  it('push to cargo and flush manully', async () => {
    for (let i = 0; i < NUM_OF_LINE; i++) {
      theCargo.push(new Date(), i, columnValueString);
    }
    await theCargo.flushToFile();
  });

  it('cargo should flush to file', () => {
    assert(fs.existsSync(theCargo.pathToCargoFile), `log file not exist on ${theCargo.pathToCargoFile}`);
  });

  it('exam content written on hd file', (done) => {
    const contentInHD = fs.readFileSync(theCargo.pathToCargoFile, 'utf8');
    const contentInHDArr = _.compact(contentInHD.split(/\r|\n|\r\n/));

    assert(contentInHDArr.length === NUM_OF_LINE,
      `unmatching output length. NUM_OF_LINE:${NUM_OF_LINE}, contentInHDArr.length:${contentInHDArr.length}`);

    for (let i = 0; i < contentInHDArr.length; i++) {
      const line = JSON.parse(contentInHDArr[i]);
      assert(line[1] === i, 'unmatching field 1');
      assert(line[2] === columnValueString, 'unmatching field 2');
    }

    // clean up cargo file
    rimrafSync(folderOfCargoFile);
    mkdirp.sync(folderOfCargoFile);
    done();
  });
});

describe('push log to cargo and flush automatically', function() {
  this.timeout(20000);

  const theCargo = createCargo(`${TABLE_NAME}_set2`);

  // clean up cargo file
  const folderOfCargoFile = path.dirname(theCargo.pathToCargoFile);
  rimrafSync(folderOfCargoFile);
  mkdirp.sync(folderOfCargoFile);

  const columnValueString = Date.now().toString(36);

  it('push to cargo and flush manully', async () => {
    for (let i = 0; i < NUM_OF_LINE; i++) {
      theCargo.push(new Date(), i, columnValueString);
    }
    await sleep(5);
  });

  it('cargo should flush to file', () => {
    assert(fs.existsSync(theCargo.pathToCargoFile), `log file not exist on ${theCargo.pathToCargoFile}`);
  });

  it('exam content written on hd file', (done) => {
    const contentInHD = fs.readFileSync(theCargo.pathToCargoFile, 'utf8');
    const contentInHDArr = _.compact(contentInHD.split(/\r|\n|\r\n/));
    debuglog('[exam hd content] contentInHDArr:', contentInHDArr);

    assert(contentInHDArr.length === NUM_OF_LINE,
      `unmatching output length. NUM_OF_LINE:${NUM_OF_LINE}, contentInHDArr.length:${contentInHDArr.length}`);

    for (let i = 0; i < contentInHDArr.length; i++) {
      const line = JSON.parse(contentInHDArr[i]);
      assert(line[1] === i, 'unmatching field 1');
      assert(line[2] === columnValueString, 'unmatching field 2');
    }

    // clean up cargo file
    rimrafSync(folderOfCargoFile);
    mkdirp.sync(folderOfCargoFile);
    done();
  });
});
