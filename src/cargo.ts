/**
 * Cargo — a write-behind buffer for ClickHouse inserts.
 *
 * Rows submitted via {@link Cargo.push} accumulate in memory, are flushed to
 * a local append-only cache file, and are periodically rotated and uploaded
 * to ClickHouse as batch INSERTs. Rotated files that could not be committed
 * (e.g. ClickHouse is down) stay on disk and are retried in later rounds, so
 * no data is lost on server outages or process crashes.
 */
import fs from "fs";
import path from "path";
import cluster from "cluster";
import assert from "assert";
import merge2 from "merge2";
import split2 from "split2";
import type { ClickHouseClient, InsertResult } from "@clickhouse/client";
import debug from "debug";
import { isThisLeader } from "./leader_election";

// Use fs.promises API for async file operations
const fsAsync = fs.promises;

/** Identifies this process in log lines and rotation file names ("nocluster" when not running under cluster). */
const CLUSTER_WORKER_ID = cluster.isPrimary ? "nocluster" : String(cluster.worker!.id);
const debuglog = debug(`chcargo:cargo@${CLUSTER_WORKER_ID}`);

const FILENAME_PREFIX = "cargo_";            // prefix of every cargo cache file
const EXTNAME_UNCOMMITTED = ".uncommitted";  // suffix of rotated files waiting to be uploaded
const MIN_TIME = 1000;                       // lower bound (ms) for the maxTime option
const MIN_ROWS = 100;                        // lower bound (rows) for the maxRows option
const DEFAULT_COMMIT_INTERVAL = 5000;        // default interval (ms) between commit rounds
const DEFAULT_MAX_INSET_PARTS = 100;         // default max number of files combined into one INSERT

/** Monotonic counter that keeps rotation file names unique within this process. */
let StaticCountWithinProcess = 0;

/**
 * ClickHouse per-query settings applied to every INSERT issued by Cargo:
 *
 * - `wait_end_of_query` — only read the HTTP response after the INSERT has
 *   fully finished server-side, so failures surface as request errors.
 * - `input_format_json_throw_on_bad_escape_sequence` — tolerate invalid
 *   escape sequences inside JSON strings instead of failing the batch.
 * - `input_format_allow_errors_num` / `input_format_allow_errors_ratio` —
 *   silently skip rows that fail to parse (at most 10 rows or 1% of the
 *   batch). Skipped rows are lost, use scripts/exam_cargo_files to audit.
 */
const CLICKHOUSE_SETTINGS = {
  wait_end_of_query: 1 as const,

  // tolerate invalid JSON escape sequences
  input_format_json_throw_on_bad_escape_sequence: 0 as const,

  // silently skip rows with other parsing errors
  input_format_allow_errors_num: "10",
  input_format_allow_errors_ratio: 0.01
};

/** Rows are serialized in ClickHouse's JSONCompactEachRow format: one JSON array of column values per line. */
const CLICKHOUSE_INPUT_FORMAT = 'JSONCompactEachRow';

/** Constructor options of a Cargo. */
export interface CargoOptions {
  /** Folder holding the cargo cache files. Required. */
  pathToCargoFolder: string;
  /** Max age (ms) of the in-memory buffer before it is flushed to file. Default 1000. */
  maxTime?: number;
  /** Max number of rows kept in memory before flushing to file. Default 100. */
  maxRows?: number;
  /** Min interval (ms) between two commit rounds to ClickHouse. Default 5000. */
  commitInterval?: number;
  /** Max number of rotation files combined into a single INSERT. Default 100. */
  maxInsetParts?: number;
}

/**
 * One Cargo buffers inserts for one ClickHouse table.
 * Instances are created through `createCargo()` (see index.ts) which guarantees
 * one shared instance per table name.
 */
export class Cargo {
  public readonly tableName: string;
  public readonly maxTime: number;
  public readonly maxRows: number;
  public readonly commitInterval: number;
  public readonly maxInsetParts: number;
  public readonly pathToCargoFolder: string;
  /** Path of the active (non-rotated) cache file that in-memory rows are flushed into. */
  public readonly pathToCargoFile: string;
  public readonly clickhouseClient: ClickHouseClient;

  /** In-memory rows, each already serialized as one JSONCompactEachRow line. */
  private cachedRows: string[] = [];
  /** Timestamp of the last flush, used together with maxTime. */
  private lastFlushAt: number = Date.now();
  /** Timestamp of the last successful commit round, used together with commitInterval. */
  private lastCommitAt: number = Date.now();
  /** Total number of rotations performed by this instance (statistics). */
  private countRotation = 0;
  /** Guard against overlapping async flushToFile calls. */
  private _isFlushing = false;
  /** Guard against overlapping async rotateFile calls. */
  private _isFileRotating = false;

  /** Compact representation used in debug log lines. */
  toString(): string {
    return `[Cargo ${this.tableName}]`;
  }

  /**
   * @param tableName - Name of the ClickHouse table to insert into
   * @param options - Configuration options
   * @param clickhouseClient - Clickhouse client instance
   */
  constructor(tableName: string, options: CargoOptions, clickhouseClient: ClickHouseClient) {
    this.tableName = tableName;
    this.maxTime = Math.max(parseInt(String(options.maxTime)) || MIN_TIME, MIN_TIME);
    this.maxRows = Math.max(parseInt(String(options.maxRows)) || MIN_ROWS, MIN_ROWS);
    this.commitInterval = Math.max(parseInt(String(options.commitInterval)) || DEFAULT_COMMIT_INTERVAL, this.maxTime);
    this.maxInsetParts = Math.max(parseInt(String(options.maxInsetParts)) || DEFAULT_MAX_INSET_PARTS, 1);

    // Configure paths and HTTP options
    assert(options.pathToCargoFolder, "missing required option: pathToCargoFolder");
    this.pathToCargoFolder = options.pathToCargoFolder;
    this.pathToCargoFile = path.join(this.pathToCargoFolder, FILENAME_PREFIX + this.tableName);

    this.clickhouseClient = clickhouseClient;

    debuglog(`[new Cargo] @tableName:${this.tableName}, @maxTime:${this.maxTime}, @maxRows:${this.maxRows}, @commitInterval:${this.commitInterval}, @pathToCargoFile:${this.pathToCargoFile}`);
  }

  /**
   * Push one row (one value per table column) into the in-memory cache.
   * Each `Date` is converted to unix seconds and each boolean to 0/1, then the
   * value array is serialized as one JSONCompactEachRow line.
   * Nothing is written to disk until the next flush.
   */
  push(...args: unknown[]): void {
    assert(args.length > 0, "blank row can not be accepted.");

    // ISSUE: No validation on data types or schema matching with ClickHouse table
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

  /**
   * Flush the in-memory cache to the disk file, but only when a threshold is
   * reached: `forced` is truthy, the buffer exceeds maxRows, or the buffer is
   * older than maxTime. On write failure the rows are put back at the front of
   * the cache so they are retried by the next flush. Concurrent calls are
   * collapsed into a no-op by the _isFlushing guard.
   */
  async flushToFile(forced?: boolean): Promise<void> {
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

  /**
   * Synchronously dump the in-memory cache to the disk file.
   * Used on process crash / SIGINT when there is no time for async I/O —
   * blocking here is acceptable because the process is about to die.
   */
  flushSync(): void {
    if (!this.cachedRows.length) {
      debuglog(`${this} [flushSync] nothing to flush`);
      return;
    }

    const rowsToFlush = this.cachedRows;
    this.cachedRows = [];

    debuglog(`${this} [flushSync] ${rowsToFlush.length} rows`);
    fs.appendFileSync(this.pathToCargoFile, rowsToFlush.join("\n") + "\n");
  }

  /**
   * Upload a single cargo file to ClickHouse as-is: the raw file bytes are
   * streamed into one INSERT (the file must contain one JSON row per line).
   */
  async uploadCargoFile(filepath: string): Promise<InsertResult> {
    debuglog(`[uploadCargoFile ${this.tableName}] filepath:`, filepath);
    return await this.clickhouseClient.insert({
      table: this.tableName,
      format: CLICKHOUSE_INPUT_FORMAT,
      values: fs.createReadStream(filepath),
      clickhouse_settings: CLICKHOUSE_SETTINGS
    });
  }

  /**
   * Upload multiple rotated files to ClickHouse in a single INSERT.
   * Every file is split into parsed JSON lines and all lines are merged into
   * one object stream, which avoids loading whole files into memory. Local
   * files are unlinked only after the INSERT succeeded, so a failed upload
   * leaves them on disk for the next retry round.
   */
  async uploadLocalUncommitsToClickhouse(filenamList: string[]): Promise<void> {
    debuglog(`[uploadLocalUncommitsToClickhouse] filenamList ${filenamList.length} :`, filenamList);

    if (!Array.isArray(filenamList) || !filenamList.length) {
      return;
    }

    const listOfFileStreams = filenamList.map(filepath =>
      fs.createReadStream(filepath, { encoding: 'utf8' }).pipe(split2(JSON.parse))
    );

    // equivalent to merge2(...listOfFileStreams, { objectMode: true }),
    // written this way to satisfy the @types/merge2 signatures for arbitrary stream count
    const combinedStream = merge2({ objectMode: true });
    for (const fileStream of listOfFileStreams) {
      combinedStream.add(fileStream);
    }
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

  /**
   * Periodic routine driving the whole write-behind pipeline:
   * flush memory to file, then — rate-limited by commitInterval and only for
   * the cluster leader — rotate the cache file and commit rotations to
   * ClickHouse. All errors are swallowed (and logged) so the routine can be
   * called forever by the scheduler in index.ts.
   */
  async exam(): Promise<void> {
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

  /**
   * Prepare uncommitted data for upload: rename the active cache file to a
   * timestamped `*.uncommitted` rotation file, so subsequent flushes start a
   * fresh file. A missing (ENOENT) or empty cache file simply means there is
   * nothing to rotate.
   * @returns true if a rotation happened, false otherwise
   */
  async rotateFile(): Promise<boolean> {
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
      if ((err as NodeJS.ErrnoException).code !== 'ENOENT') {
        debuglog("[rotateFile] ABORT fail to stats file. error:", err);
      }
      return false;
    } finally {
      this._isFileRotating = false;
    }
  }

  /**
   * Commit local rotated files to the remote ClickHouse DB: scan the cargo
   * folder for this table's `*.uncommitted` rotations, sort them from newest
   * to oldest, cap the list at maxInsetParts, and upload them in one batch.
   * Older rotations beyond the cap are picked up by the next round.
   */
  async commitToClickhouseDB(): Promise<void> {
    let filenamList: string[];
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
      const tsA = parseInt((a.split(splitMark)[1] || "0"), 36);
      const tsB = parseInt((b.split(splitMark)[1] || "0"), 36);
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
