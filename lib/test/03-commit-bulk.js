// Generated by CoffeeScript 2.5.1
(function() {
  var ClickHouse, INIT_OPTION, NUM_OF_LINE, STATEMENT_CREATE_TABLE, STATEMENT_DROP_TABLE, STATEMENT_SELECT, TABLE_NAME, assert, columnValueString, createCargo, debuglog, fs, getClickHouseClient, isInited, os, path, sleep;

  ({createCargo, isInited, getClickHouseClient} = require("../"));

  debuglog = require("debug")("chcargo:test:03");

  ({sleep} = require("../utils"));

  assert = require("assert");

  fs = require("fs");

  os = require("os");

  path = require("path");

  ClickHouse = require('@apla/clickhouse');

  TABLE_NAME = "cargo_test.unittest03";

  STATEMENT_CREATE_TABLE = `CREATE TABLE IF NOT EXISTS ${TABLE_NAME}
(
  \`time\` DateTime ,
  \`step\`  UInt32,
  \`pos_id\` String DEFAULT ''
)
ENGINE = Memory()`;

  STATEMENT_CREATE_TABLE = STATEMENT_CREATE_TABLE.replace(/\n|\r/g, ' ');

  STATEMENT_DROP_TABLE = `DROP TABLE IF EXISTS ${TABLE_NAME}`;

  columnValueString = Date.now().toString(36);

  STATEMENT_SELECT = `SELECT * FROM ${TABLE_NAME} WHERE pos_id='${columnValueString}' LIMIT 100000 FORMAT JSONCompactEachRow `;

  // refer
  INIT_OPTION = {
    host: "localhost",
    maxTime: 2000,
    maxRows: 100,
    commitInterval: 8000
  };

  NUM_OF_LINE = 27891; // NOTE: bulk flushs every 100 lines

  //NUM_OF_LINE = 9  # NOTE: bulk flushs every 100 lines
  getClickHouseClient = function() {
    var err, pathToConfig, profileConfig, profileName;
    profileName = process.env.CLICKHOUSE_CARGO_PROFILE;
    assert(profileName, "missing process.env.CLICKHOUSE_CARGO_PROFIL");
    if (path.extname(profileName) !== ".json") {
      profileName += ".json";
    }
    pathToConfig = path.join(os.homedir(), ".clickhouse-cargo", profileName);
    debuglog(`[getClickHouseClient] try auto init from CLICKHOUSE_CARGO_PROFILE from ${pathToConfig}`);
    try {
      profileConfig = JSON.parse(fs.readFileSync(pathToConfig));
    } catch (error) {
      err = error;
      debuglog("[static init] FAILED error:", err);
    }
    return new ClickHouse(profileConfig);
  };

  describe("commit bulk", function() {
    var clickHouseClient, theCargo;
    this.timeout(30000);
    clickHouseClient = getClickHouseClient();
    theCargo = null;
    before(function(done) {
      debuglog("[before]");
      clickHouseClient.query(STATEMENT_DROP_TABLE, function(err) {
        if (err != null) {
          throw err;
        }
        clickHouseClient.query(STATEMENT_CREATE_TABLE, function(err) {
          if (err != null) {
            throw err;
          }
          theCargo = createCargo(TABLE_NAME);
          if (fs.existsSync(theCargo.pathToCargoFile)) { // clean up existing log
            // receive notification when commit failed
            //theCargo.on 'error', (err)->
            //debuglog "[on cargo error] error:", err
            //return
            fs.unlinkSync(theCargo.pathToCargoFile);
          }
          done();
        });
      });
    });
    //after -> process.exit(0)
    it("push to cargo", async function() {
      var i, j, ref;
      for (i = j = 0, ref = NUM_OF_LINE; (0 <= ref ? j < ref : j > ref); i = 0 <= ref ? ++j : --j) {
        theCargo.push(new Date(), i, columnValueString);
      }
      await sleep(20); // wait file stream flush
    });
    it("bulk should committed", function(done) {
      assert(!fs.existsSync(theCargo.pathToCargoFile), "local file must be cleared");
      done();
    });
    //it "push to cargo", ->
    //for i in [0...NUM_OF_LINE]
    //theCargo.push(new Date, i, columnValueString)

    //await sleep 10 # wait file stream flush
    //return
    it("records should be written into remote ClickHouse server", function(done) {
      var rows, stream;
      rows = [];
      debuglog("[read db] select:", STATEMENT_SELECT);
      stream = clickHouseClient.query(STATEMENT_SELECT, {
        format: "JSONCompactEachRow"
      }, function(err, result) {
        var i, j, len, row;
        if (err != null) {
          done(err);
          return;
        }
        result = result.replace(/\n/g, ",").trim().replace(/,$/, '');
        result = `[ ${result} ]`;
        console.dir(result);
        result = JSON.parse(result);
        debuglog("[read db] result:", result.length);
        console.dir(result, {
          depth: 10
        });
        assert(result.length === NUM_OF_LINE, `unmatching row length local:${NUM_OF_LINE}, remote:${result.length}`);
        result.sort(function(a, b) {
          return a[1] - b[1];
        });
        for (i = j = 0, len = result.length; j < len; i = ++j) {
          row = result[i];
          assert(row[1] === i, "unmatching field 1 ");
          assert(row[2] === columnValueString, "unmatching field 2 ");
        }
        done();
      });
    });
  });

}).call(this);
