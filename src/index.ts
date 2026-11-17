/**
 * clickhouse-cargo entry point and public API.
 *
 * Responsibilities:
 * - `init()` — create the shared ClickHouse client, resolve the cargo cache
 *   folder, and register crash/stop handlers that flush in-memory rows.
 * - `createCargo()` — create (or return the existing) Cargo bound to a table.
 * - A background "exam" loop that periodically drives every cargo's
 *   flush → rotate → commit pipeline.
 * - Optional auto-init from `~/.clickhouse-cargo/${CLICKHOUSE_CARGO_PROFILE}.json`.
 */
import assert from "assert";
import path from "path";
import os from "os";
import fs from "fs";
import cluster from "cluster";
import { createClient } from "@clickhouse/client";
import type { ClickHouseClient, ClickHouseClientConfigOptions } from "@clickhouse/client";
import debug from "debug";
import { Cargo, detectBadEscapeSupport } from "./cargo";
import type { CargoOptions as ICargoOptions } from "./cargo";

/** Identifies this process in log lines ("nocluster" when not running under cluster). */
const CLUSTER_WORKER_ID = cluster.isPrimary ? "nocluster" : String(cluster.worker!.id);
const debuglog = debug(`chcargo:index@${CLUSTER_WORKER_ID}`);

/** Table names are restricted to word chars, digits, dots, dashes and underscores. */
const REG_INVALID_SQL_TABLE_NAME_CHAR = /[^\w\d\.\-_]/i;

// KV map: tableName -> cargo instance
const TABLE_NAME_TO_CARGO: Record<string, Cargo> = {};

// Global configuration options shared by all cargos, filled in by init()
const CargoOptions: Partial<ICargoOptions> = {};

// Global Clickhouse client instance, created by init()
let ClickhouseClient: ClickHouseClient | null = null;

/** Configuration of `init()`: @clickhouse/client options plus cargo-specific tuning. */
export interface InitConfig extends Omit<ClickHouseClientConfigOptions, "url"> {
  /** ClickHouse server base URL. Required. */
  url: string;
  /** Folder for cargo cache files, resolved against cwd. Default "cargo_files". */
  pathToCargoFolder?: string;
  /** Max age (ms) of in-memory buffers before flushing to file. Default 1000. */
  maxTime?: number;
  /** Max number of rows kept in memory per cargo. Default 100. */
  maxRows?: number;
  /** Min interval (ms) between commit rounds. Default 5000. */
  commitInterval?: number;
  /** Max rotation files combined into one INSERT. Default 100. */
  maxInsetParts?: number;
  /** Set `false` to skip flushing in-memory rows on process crash / SIGINT. Default true. */
  saveWhenCrash?: boolean;
}

/**
 * Initialize the Clickhouse cargo system. Must be called (directly or via the
 * CLICKHOUSE_CARGO_PROFILE environment variable) before `createCargo()`.
 * Throws when the cargo folder is missing or not writable.
 * @param config Configuration object
 */
function init(config: InitConfig): void {
  assert(config, "missing config");
  assert(config.url, "missing url in config");

  // refer to https://clickhouse.com/docs/en/integrations/javascript
  const clickhouseClientOption: ClickHouseClientConfigOptions = {
    url: config.url,
    pathname: config.pathname,
    request_timeout: config.request_timeout,
    compression: config.compression,
    username: config.username,
    password: config.password,
    application: "clickhouse-cargo",
    database: config.database,
    log: config.log,
    http_headers: config.http_headers,
    role: config.role,
    max_open_connections: config.max_open_connections,
    tls: config.tls,
    keep_alive: config.keep_alive,
    set_basic_auth_header: config.set_basic_auth_header
  };

  ClickhouseClient = createClient(clickhouseClientOption);

  // Warm up the server-version probe in the background: the result decides
  // whether INSERTs may carry the bad-escape tolerance setting (v24.4+).
  // detectBadEscapeSupport never rejects and retries on demand, so a down
  // server at startup is harmless here.
  void detectBadEscapeSupport(ClickhouseClient);

  // prepare disk path
  CargoOptions.pathToCargoFolder = path.resolve(process.cwd(), config.pathToCargoFolder || "cargo_files");

  // verify cargo can write to the destination folder and is a directory
  try {
    fs.accessSync(CargoOptions.pathToCargoFolder, fs.constants.W_OK);
    const stats = fs.statSync(CargoOptions.pathToCargoFolder);
    if (!stats.isDirectory()) {
      throw new Error(`Not a directory: ${CargoOptions.pathToCargoFolder}`);
    }
  } catch (err) {
    throw new Error(`Failed to access cargo folder: ${err instanceof Error ? err.message : String(err)}`);
  }

  // optional tuning knobs; values <= 0 (or unparsable) keep the Cargo defaults
  const maxTime = parseInt(String(config.maxTime));
  if (maxTime > 0) {
    CargoOptions.maxTime = maxTime;
  }

  const maxRows = parseInt(String(config.maxRows));
  if (maxRows > 0) {
    CargoOptions.maxRows = maxRows;
  }

  const commitInterval = parseInt(String(config.commitInterval));
  if (commitInterval > 0) {
    CargoOptions.commitInterval = commitInterval;
  }

  const maxInsetParts = parseInt(String(config.maxInsetParts));
  if (maxInsetParts > 0) {
    CargoOptions.maxInsetParts = maxInsetParts;
  }

  // flush in-memory rows to disk when the process is about to die
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
 * @param tableName The name of ClickHouse table which data is inserted
 * @returns The cargo instance
 */
function createCargo(tableName?: string): Cargo {
  debuglog(`[createCargo] tableName:${tableName}`);
  assert(ClickhouseClient, "ClickHouse-Cargo needs to be inited first");

  const theTableName = String(tableName || "").trim();
  assert(theTableName && !REG_INVALID_SQL_TABLE_NAME_CHAR.test(theTableName), `invalid tableName:${theTableName}`);

  let cargo = TABLE_NAME_TO_CARGO[theTableName];
  if (cargo) {
    debuglog(`[createCargo] reuse cargo@${theTableName}:`, cargo.toString());
    return cargo;
  }

  // init() is guaranteed to have assigned pathToCargoFolder before reaching here,
  // because the assert above requires ClickhouseClient which is only created in init().
  cargo = new Cargo(theTableName, CargoOptions as ICargoOptions, ClickhouseClient);
  TABLE_NAME_TO_CARGO[theTableName] = cargo;
  debuglog(`[createCargo] cargo@${theTableName}:`, cargo.toString());
  return cargo;
}

/** Run one exam round on a single cargo and log slow rounds (>100ms). Never throws. */
async function examOneCargo(cargo: Cargo): Promise<void> {
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

/**
 * The endless background routine: examine every cargo sequentially, then
 * reschedule itself via setImmediate, padded to roughly one round per second.
 */
async function examCargos(): Promise<void> {
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
    await new Promise<void>(resolve => setTimeout(resolve, 1000 - msSpent));
  }

  setImmediate(async () => await examCargos());
}

// Static initialization from environment:
// auto-init when CLICKHOUSE_CARGO_PROFILE is set, reading the profile JSON
// from ~/.clickhouse-cargo/<profile>.json. Recommended for production so the
// connection config never lives in application code. Failures are logged and
// swallowed — the app may still call init() manually later.
const CLICKHOUSE_CARGO_PROFILE_ENV = process.env.CLICKHOUSE_CARGO_PROFILE;
if (CLICKHOUSE_CARGO_PROFILE_ENV) {
  let profileName = CLICKHOUSE_CARGO_PROFILE_ENV;
  if (path.extname(profileName) !== ".json") {
    profileName += ".json";
  }

  const pathToConfig = path.join(os.homedir(), ".clickhouse-cargo", CLICKHOUSE_CARGO_PROFILE_ENV + ".json");
  debuglog("[static init] try auto init from CLICKHOUSE_CARGO_PROFILE");

  try {
    const profileConfig = JSON.parse(fs.readFileSync(pathToConfig, "utf8")) as InitConfig;
    init(profileConfig);
  } catch (err) {
    debuglog("[static init] FAILED error:", err);
  }
}

// Start examination routine (small delay so require() can settle first)
setTimeout(async () => await examCargos(), 100);

/** Whether init() has already run successfully. */
const isInited = (): boolean => !!ClickhouseClient;

/** The shared ClickHouse client created by init(), or null before init. */
const getClickhouseClient = (): ClickHouseClient | null => ClickhouseClient;

export {
  init,
  createCargo,
  isInited,
  getClickhouseClient
};
