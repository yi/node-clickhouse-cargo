/**
 * Small shared helpers used across the module and its tests.
 */
import assert from "assert";

/**
 * Format a Date as a "YYYY-MM-DD HH:mm:ss" string in UTC — the literal
 * format accepted by ClickHouse DateTime columns.
 */
export function toSQLDateString(date: Date): string {
  return date.getUTCFullYear() + '-' +
    ('00' + (date.getUTCMonth() + 1)).slice(-2) + '-' +
    ('00' + date.getUTCDate()).slice(-2) + ' ' +
    ('00' + date.getUTCHours()).slice(-2) + ':' +
    ('00' + date.getUTCMinutes()).slice(-2) + ':' +
    ('00' + date.getUTCSeconds()).slice(-2);
}

/**
 * Sleep for the given number of seconds.
 * The duration is logged to stdout, mainly for test observability.
 */
export function sleep(seconds: number): Promise<void> {
  console.log(`sleep ${seconds} seconds`);
  assert(Number.isInteger(seconds) && seconds > 0);
  return new Promise((resolve) => {
    setTimeout(resolve, seconds * 1000);
  });
}
