const fs = require("fs");
const path = require("path");
const cluster = require('cluster');
const assert = require("assert");
const { isThisLeader } = require("./leader_election");
const merge2 = require('merge2');
const split2 = require('split2');

// Use fs.promises API for async file operations
const fsAsync = fs.promises;

const CLUSTER_WORKER_ID = cluster.isMaster ? "nocluster" : cluster.worker.id;
const debuglog = require("debug")(`chcargo:cargo@${CLUSTER_WORKER_ID}`);

const FILENAME_PREFIX = "cargo_";
const EXTNAME_UNCOMMITTED = ".uncommitted";
const MAX_COMMIT_PER_EXAM_ROUTINE = 1;
const MIN_TIME = 1000;
const MIN_ROWS = 100;
const DEFAULT_COMMIT_INTERVAL = 5000;
const DEFAULT_MAX_INSET_PARTS = 100;

let StaticCountWithinProcess = 0;

const CLICKHOUSE_SETTINGS = {
  wait_end_of_query: 1
};

const CLICKHOUSE_INPUT_FORMAT = 'JSONCompactEachRow';

class Cargo {
  toString() {
    return `[Cargo ${this.tableName}]`;
  }

  /**
   * @param {string} tableName - Name of the ClickHouse table to insert into
   * @param {Object} options - Configuration options
   * @param {string} options.pathToCargoFolder - Path to folder for storing cargo files
   * @param {number} [options.maxTime=1000] - Max time in ms before flushing cached rows
   * @param {number} [options.maxRows=100] - Max number of rows to cache before flushing
   * @param {number} [options.commitInterval=5000] - Interval in ms between commits to ClickHouse
   * @param {number} [options.maxInsetParts=100] - Max number of files to combine in one insert
   * @param {Object} [clickhouseClient] - Clickhouse client instance
   */
  constructor(tableName, options = {}, clickhouseClient) {
    this.tableName = tableName;
    this.maxTime = Math.max(parseInt(options.maxTime) || MIN_TIME, MIN_TIME);
    this.maxRows = Math.max(parseInt(options.maxRows) || MIN_ROWS, MIN_ROWS);
    this.commitInterval = Math.max(parseInt(options.commitInterval) || DEFAULT_COMMIT_INTERVAL, this.maxTime);
    this.maxInsetParts = Math.max(parseInt(options.maxInsetParts) || DEFAULT_MAX_INSET_PARTS, 1);

    // Configure paths and HTTP options
    assert(options.pathToCargoFolder, "missing required option: pathToCargoFolder");
    this.pathToCargoFolder = options.pathToCargoFolder;
    this.pathToCargoFile = path.join(this.pathToCargoFolder, FILENAME_PREFIX + this.tableName);

    // TODO: Consider adding retry mechanism for failed inserts
    this.clickhouseClient = clickhouseClient;

    debuglog(`[new Cargo] @tableName:${this.tableName}, @maxTime:${this.maxTime}, @maxRows:${this.maxRows}, @commitInterval:${this.commitInterval}, @pathToCargoFile:${this.pathToCargoFile}`);

    // Initialize state
    this.cachedRows = [];
    this.lastFlushAt = Date.now();
    this.lastCommitAt = Date.now();
    this.countRotation = 0;
  }

  // Push row insert into memory cache
  push(...args) {
    assert(args.length > 0, "blank row can not be accepted.");

    const processedArgs = args.map(item => {
      if (item instanceof Date) {
        return Math.round(item.getTime() / 1000);
      } else if (typeof item === "boolean") {
        return Number(item);
      }
      return item;
    });

    this.cachedRows.push(JSON.stringify(processedArgs));
  }

  // Flush memory cache to the disk file
  async flushToFile(forced) {
    if (this._isFlushing) {
      return;
    }

    if (!this.cachedRows.length) {
      this.lastFlushAt = Date.now();
      return;
    }

    if (!(forced || (this.cachedRows.length > this.maxRows) || (Date.now() > this.lastFlushAt + this.maxTime))) {
      debuglog(`${this} [flushToFile] SKIP threshold not reach`);
      return;
    }

    this._isFlushing = true;
    const rowsToFlush = this.cachedRows;
    this.cachedRows = [];

    debuglog(`${this} [flushToFile] -> ${rowsToFlush.length} rows`);

    try {
      await fsAsync.appendFile(this.pathToCargoFile, rowsToFlush.join("\n") + "\n");
      debuglog(`${this} [flushToFile] SUCCESS ${rowsToFlush.length} rows`);
    } catch (err) {
      debuglog(`${this} [flushToFile] ${rowsToFlush.length} rows FAILED error:`, err);
      this.cachedRows = rowsToFlush.concat(this.cachedRows); // unshift data back
    }

    this.lastFlushAt = Date.now();
    this._isFlushing = false;
  }

  // Flush memory cache to the disk file synchronously
  flushSync() {
    if (!this.cachedRows.length) {
      debuglog(`${this} [flushSync] nothing to flush`);
      return;
    }

    const rowsToFlush = this.cachedRows;
    this.cachedRows = [];

    debuglog(`${this} [flushSync] ${rowsToFlush.length} rows`);
    fs.appendFileSync(this.pathToCargoFile, rowsToFlush.join("\n") + "\n");
  }

  // Upload a single cargo file to clickhouse
  async uploadCargoFile(filepath) {
    debuglog(`[uploadCargoFile ${this.tableName}] filepath:`, filepath);
    return await this.clickhouseClient.insert({
      table: this.tableName,
      format: CLICKHOUSE_INPUT_FORMAT,
      values: fs.createReadStream(filepath),
      clickhouse_settings: CLICKHOUSE_SETTINGS
    });
  }

  // Upload multiple local uncommits to clickhouse
  async uploadLocalUncommitsToClickhouse(filenamList) {
    debuglog(`[uploadLocalUncommitsToClickhouse] filenamList ${filenamList.length} :`, filenamList);

    if (!Array.isArray(filenamList) || !filenamList.length) {
      return;
    }

    const listOfFileStreams = filenamList.map(filepath =>
      fs.createReadStream(filepath, { encoding: 'utf8' }).pipe(split2(JSON.parse))
    );

    const combinedStream = merge2(...listOfFileStreams, { objectMode: true });
    debuglog(`[uploadLocalUncommitsToClickhouse] combinedStream  ${listOfFileStreams.length}`);

    await this.clickhouseClient.insert({
      table: this.tableName,
      format: CLICKHOUSE_INPUT_FORMAT,
      values: combinedStream,
      clickhouse_settings: CLICKHOUSE_SETTINGS
    });

    // Remove successfully committed local files
    for (const filepath of filenamList) {
      debuglog("[uploadLocalUncommitsToClickhouse] clean local commit:", path.basename(filepath));
      await fsAsync.unlink(filepath);
    }
  }

  // Check if to commit disk file to clickhouse DB
  async exam() {
    try {
      await this.flushToFile();
    } catch (err) {
      debuglog(`[exam ${this.tableName}] ABORT fail to flush. error:`, err);
      return;
    }

    if (!(Date.now() > this.lastCommitAt + this.commitInterval)) {
      return;
    }

    if (!isThisLeader()) {
      debuglog(`[exam ${this.tableName}] CANCLE NOT leadWorkerId`);
      // non-leader skip 10 commit rounds
      this.lastCommitAt = Date.now() + this.commitInterval * 10;
      return;
    }

    try {
      const hasRotation = await this.rotateFile();
      this.countRotation += Number(hasRotation);
      await this.commitToClickhouseDB();
      this.lastCommitAt = Date.now();
    } catch (err) {
      debuglog(`[exam ${this.tableName}] FAILED to commit. error:`, err);
    }
  }

  // Prepare all uncommitted local files
  // @return {Promise<boolean>} true if there are local uncommits exist
  async rotateFile() {
    if (this._isFileRotating) {
      debuglog("[rotateFile] SKIP @_isFileRotating");
      return false;
    }

    this._isFileRotating = true;

    try {
      const stats = await fsAsync.stat(this.pathToCargoFile);

      if (!stats || stats.size === 0) {
        debuglog("[rotateFile] SKIP empty file.");
        return false;
      }

      const pathToRenameFile = path.join(
        this.pathToCargoFolder,
        `${FILENAME_PREFIX}${this.tableName}.${Date.now().toString(36)}_${++StaticCountWithinProcess}.${CLUSTER_WORKER_ID}${EXTNAME_UNCOMMITTED}`
      );

      debuglog(`[rotateFile] rotate to ${pathToRenameFile}`);
      await fsAsync.rename(this.pathToCargoFile, pathToRenameFile);

      return true;

    } catch (err) {
      if (err.code !== 'ENOENT') {
        debuglog("[rotateFile] ABORT fail to stats file. error:", err);
      }
      return false;
    } finally {
      this._isFileRotating = false;
    }
  }

  // Commit local rotated files to remote ClickHouse DB
  async commitToClickhouseDB() {
    let filenamList;
    try {
      filenamList = await fsAsync.readdir(this.pathToCargoFolder);
    } catch (err) {
      debuglog("[commitToClickhouseDB > ls] FAILED error:", err);
      return;
    }

    if (!Array.isArray(filenamList) || !filenamList.length) {
      return;
    }

    const rotationPrefix = FILENAME_PREFIX + this.tableName + '.';
    filenamList = filenamList.filter(item =>
      item.startsWith(rotationPrefix) && item.endsWith(EXTNAME_UNCOMMITTED)
    );

    if (!filenamList.length) {
      return;
    }

    debuglog(`[commitToClickhouseDB] filenamList(${filenamList.length})`);

    // Sort filenamList from new to old
    const splitMark = `${FILENAME_PREFIX}${this.tableName}.`;
    filenamList.sort((a, b) => {
      const tsA = parseInt((a.split(splitMark)[1] || 0), 36);
      const tsB = parseInt((b.split(splitMark)[1] || 0), 36);
      return tsB - tsA;
    });

    // Truncate filenamList if too long
    filenamList = filenamList.slice(0, this.maxInsetParts);

    // Add full file paths
    filenamList = filenamList.map(item => path.join(this.pathToCargoFolder, item));

    // Submit each local uncommit
    await this.uploadLocalUncommitsToClickhouse(filenamList);
  }
}

module.exports = Cargo;
