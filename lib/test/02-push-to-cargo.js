// Generated by CoffeeScript 2.5.1
(function() {
  var ClickHouse, INIT_OPTION, NUM_OF_LINE, STATEMENT_CREATE_TABLE, STATEMENT_DROP_TABLE, STATEMENT_INSERT, TABLE_NAME, _, assert, createCargo, debuglog, fs, isInited, os, path, sleep;

  ({createCargo, isInited} = require("../"));

  debuglog = require("debug")("chcargo:test:02");

  ({createCargo, isInited} = require("../"));

  assert = require("assert");

  fs = require("fs");

  os = require("os");

  path = require("path");

  _ = require("lodash");

  ClickHouse = require('@apla/clickhouse');

  ({sleep} = require("../utils"));

  TABLE_NAME = "cargo_test.unittest02";

  STATEMENT_INSERT = `INSERT INTO ${TABLE_NAME}`;

  STATEMENT_DROP_TABLE = `DROP TABLE IF EXISTS ${TABLE_NAME}`;

  STATEMENT_CREATE_TABLE = `CREATE TABLE IF NOT EXISTS ${TABLE_NAME}
(
  \`time\` DateTime ,
  \`step\`  UInt32,
  \`pos_id\` String DEFAULT ''
)
ENGINE = Memory()`;

  STATEMENT_CREATE_TABLE = STATEMENT_CREATE_TABLE.replace(/\n|\r/g, ' ');

  // refer
  INIT_OPTION = {
    host: "localhost",
    maxTime: 2000,
    maxRows: 100,
    commitInterval: 20000
  };

  NUM_OF_LINE = 429; // NOTE: bulk flushs every 100 lines

  describe("push log to cargo and flush manully", function() {
    var columnValueString, theCargo;
    this.timeout(20000);
    theCargo = createCargo(TABLE_NAME);
    if (fs.existsSync(theCargo.pathToCargoFile)) { // clean up existing log
      fs.unlinkSync(theCargo.pathToCargoFile);
    }
    columnValueString = Date.now().toString(36);
    it("push to cargo and flush manully", async function() {
      var i, j, ref;
      for (i = j = 0, ref = NUM_OF_LINE; (0 <= ref ? j < ref : j > ref); i = 0 <= ref ? ++j : --j) {
        theCargo.push(new Date(), i, columnValueString);
      }
      await theCargo.flushToFile();
    });
    it("cargo should flush to file", function() {
      assert(fs.existsSync(theCargo.pathToCargoFile), `log file not exist on ${theCargo.pathToCargoFile}`);
    });
    return it("exam content written on hd file", function(done) {
      var contentInHD, contentInHDArr, i, j, len, line;
      contentInHD = fs.readFileSync(theCargo.pathToCargoFile, 'utf8');
      //debuglog "[exam hd content] contentInHD:", contentInHD
      contentInHDArr = _.compact(contentInHD.split(/\r|\n|\r\n/));
      //debuglog "[exam hd content] contentInHDArr:", contentInHDArr
      assert(contentInHDArr.length === NUM_OF_LINE, `unmatching output length. NUM_OF_LINE:${NUM_OF_LINE}, contentInHDArr.length:${contentInHDArr.length}`);
      for (i = j = 0, len = contentInHDArr.length; j < len; i = ++j) {
        line = contentInHDArr[i];
        line = JSON.parse(line);
        //console.log line
        assert(line[1] === i, "unmatching field 1 ");
        assert(line[2] === columnValueString, "unmatching field 2 ");
      }
      done();
    });
  });

  describe("push log to cargo and flush automatically", function() {
    var columnValueString, theCargo;
    this.timeout(20000);
    theCargo = createCargo(`${TABLE_NAME}_set2`);
    if (fs.existsSync(theCargo.pathToCargoFile)) { // clean up existing log
      fs.unlinkSync(theCargo.pathToCargoFile);
    }
    columnValueString = Date.now().toString(36);
    it("push to cargo and flush manully", async function() {
      var i, j, ref;
      for (i = j = 0, ref = NUM_OF_LINE; (0 <= ref ? j < ref : j > ref); i = 0 <= ref ? ++j : --j) {
        theCargo.push(new Date(), i, columnValueString);
      }
      await sleep(5);
    });
    it("cargo should flush to file", function() {
      assert(fs.existsSync(theCargo.pathToCargoFile), `log file not exist on ${theCargo.pathToCargoFile}`);
    });
    return it("exam content written on hd file", function(done) {
      var contentInHD, contentInHDArr, i, j, len, line;
      contentInHD = fs.readFileSync(theCargo.pathToCargoFile, 'utf8');
      //debuglog "[exam hd content] contentInHD:", contentInHD
      contentInHDArr = _.compact(contentInHD.split(/\r|\n|\r\n/));
      debuglog("[exam hd content] contentInHDArr:", contentInHDArr);
      assert(contentInHDArr.length === NUM_OF_LINE, `unmatching output length. NUM_OF_LINE:${NUM_OF_LINE}, contentInHDArr.length:${contentInHDArr.length}`);
      for (i = j = 0, len = contentInHDArr.length; j < len; i = ++j) {
        line = contentInHDArr[i];
        line = JSON.parse(line);
        //console.log line
        assert(line[1] === i, "unmatching field 1 ");
        assert(line[2] === columnValueString, "unmatching field 2 ");
      }
      done();
    });
  });

}).call(this);
