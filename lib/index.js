const assert = require("assert");
const path = require("path");
const os = require("os");
const fs = require("fs");
const cluster = require('cluster');
const { createClient } = require('@clickhouse/client');

const CLUSTER_WORKER_ID = cluster.isMaster ? "nocluster" : cluster.worker.id;
const debuglog = require("debug")(`chcargo:index@${CLUSTER_WORKER_ID}`);
const Cargo = require("./cargo");
const REG_INVALID_SQL_TABLE_NAME_CHAR = /[^\w\d\.\-_]/i;

// KV map: tableName -> cargo instance
const TABLE_NAME_TO_CARGO = {};

// Global configuration options for all cargos
const CargoOptions = {};

// Global Clickhouse client instance
let ClickhouseClient = null;

/**
 * Initialize the Clickhouse cargo system
 * @param {Object} config Configuration object
 * @param {string} [config.pathToCargoFolder="cargo_files"] Path to folder where cargo files are stored
 * @param {number} [config.maxTime=10000] Max time in ms before flushing cached rows
 * @param {number} [config.maxRows=100] Max number of rows to cache before flushing
 * @param {number} [config.commitInterval=5000] Interval in ms between commits to ClickHouse
 * @param {number} [config.maxInsetParts=100] Max number of files to combine in one insert
 */
function init(config) {
    assert(config, "missing host in config");

    // refer to https://clickhouse.com/docs/en/integrations/javascript
    const clickhouseClientOption = {
        url: config.url,
        pathname: config.pathname,
        request_timeout: config.request_timeout,
        compression: config.compression,
        username: config.username,
        password: config.password,
        application: config.application,
        database: config.database,
        log: config.log,
        http_headers: config.http_headers,
        roles: config.roles,
        max_open_connections: config.max_open_connections,
        tls: config.tls,
        keep_alive: config.keep_alive,
        set_basic_auth_header: config.set_basic_auth_header
    };

    ClickhouseClient = createClient(clickhouseClientOption);

    // prepare disk path
    CargoOptions.pathToCargoFolder = path.resolve(process.cwd(), config.cargoPath || "cargo_files");
    delete config.cargoPath;

    // verify cargo can write to the destination folder
    fs.accessSync(CargoOptions.pathToCargoFolder, fs.constants.W_OK);
    fs.stat(CargoOptions.pathToCargoFolder, (err, stats) => {
        assert(err == null, `Fail to read directory stats. Due to ${err}`);
        assert(stats.isDirectory(), `Not a directory: ${CargoOptions.pathToCargoFolder}`);
    });

    const maxTime = parseInt(config.maxTime);
    if (maxTime > 0) {
        CargoOptions.maxTime = maxTime;
    }

    const maxRows = parseInt(config.maxRows);
    if (maxRows > 0) {
        CargoOptions.maxRows = maxRows;
    }

    const commitInterval = parseInt(config.commitInterval);
    if (commitInterval > 0) {
        CargoOptions.commitInterval = commitInterval;
    }

    const maxInsetParts = parseInt(config.maxInsetParts);
    if (maxInsetParts > 0) {
        CargoOptions.maxInsetParts = maxInsetParts;
    }

    const isToFlushBeforeCrash = config.saveWhenCrash === false ? false : true;
    if (isToFlushBeforeCrash) {
        // flush in-memory data when process crash
        process.on('uncaughtException', (err) => {
            debuglog("⚠️⚠️⚠️  [flushSyncInMemoryCargo] ⚠️⚠️⚠️  ");
            for (const tableName in TABLE_NAME_TO_CARGO) {
                const cargo = TABLE_NAME_TO_CARGO[tableName];
                cargo.flushSync();
            }
            throw err;
        });

        // https://pm2.keymetrics.io/docs/usage/signals-clean-restart/
        process.on('SIGINT', () => {
            console.log("⚠️⚠️⚠️  [PM2 STOP SIGNAL] ⚠️⚠️⚠️  ");
            for (const tableName in TABLE_NAME_TO_CARGO) {
                const cargo = TABLE_NAME_TO_CARGO[tableName];
                cargo.flushSync();
            }
            setImmediate(() => process.exit());
        });
    }
}

/**
 * Create a cargo instance.
 * Cargos are bound to table. Call create multiple times with the same table name will result in one shared cargo.
 * @param {string} tableName The name of ClickHouse table which data is inserted
 * @returns {Cargo} The cargo instance
 */
function createCargo(tableName) {
    debuglog(`[createCargo] tableName:${tableName}`);
    assert(ClickhouseClient, "ClickHouse-Cargo needs to be inited first");

    tableName = String(tableName || "").trim();
    assert(tableName && !REG_INVALID_SQL_TABLE_NAME_CHAR.test(tableName), `invalid tableName:${tableName}`);

    let cargo = TABLE_NAME_TO_CARGO[tableName];
    if (cargo) {
        debuglog(`[createCargo] reuse cargo@${tableName}:`, cargo.toString());
        return cargo;
    }

    cargo = new Cargo(tableName, CargoOptions, ClickhouseClient);
    TABLE_NAME_TO_CARGO[tableName] = cargo;
    debuglog(`[createCargo] cargo@${tableName}:`, cargo.toString());
    return cargo;
}

async function examOneCargo(cargo) {
    try {
        const examStartAt = Date.now();
        await cargo.exam(); // one-by-one
        const msSpent = Date.now() - examStartAt;
        if (msSpent > 100) {
            debuglog(`[examOneCargo] ${cargo.tableName} takes: ${msSpent}ms`);
        }
    } catch (err) {
        debuglog(`[examOneCargo] ${cargo.tableName} FAILED error:`, err);
    }
}

async function examCargos() {
    const routineStartAt = Date.now();

    for (const tableName in TABLE_NAME_TO_CARGO) {
        const cargo = TABLE_NAME_TO_CARGO[tableName];
        await examOneCargo(cargo);
    }

    const msSpent = Date.now() - routineStartAt;
    if (msSpent > 100) {
        debuglog(`[examCargos] rountine takes ${msSpent}ms`);
    }

    if (msSpent < 1000) {
        // sleep till next seconds
        await new Promise(resolve => setTimeout(resolve, 1000 - msSpent));
    }

    setImmediate(async () => await examCargos());
}

// Static initialization from environment
if (process.env.CLICKHOUSE_CARGO_PROFILE) {
    let profileName = process.env.CLICKHOUSE_CARGO_PROFILE;
    if (path.extname(profileName) !== ".json") {
        profileName += ".json";
    }

    const pathToConfig = path.join(os.homedir(), ".clickhouse-cargo", process.env.CLICKHOUSE_CARGO_PROFILE + ".json");
    debuglog("[static init] try auto init from CLICKHOUSE_CARGO_PROFILE");

    try {
        const profileConfig = JSON.parse(fs.readFileSync(pathToConfig));
        init(profileConfig);
    } catch (err) {
        debuglog("[static init] FAILED error:", err);
    }
}

// Start examination routine
setTimeout(async () => await examCargos(), 100);

module.exports = {
    init,
    createCargo,
    isInited: () => !!ClickhouseClient,
    getClickhouseClient: () => ClickhouseClient
};
