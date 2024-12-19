const assert = require("assert");

function toSQLDateString(date) {
  //debuglog "[toSQLDateString] date:", date
  return date.getUTCFullYear() + '-' +
    ('00' + (date.getUTCMonth() + 1)).slice(-2) + '-' +
    ('00' + date.getUTCDate()).slice(-2) + ' ' +
    ('00' + date.getUTCHours()).slice(-2) + ':' +
    ('00' + date.getUTCMinutes()).slice(-2) + ':' +
    ('00' + date.getUTCSeconds()).slice(-2);
}

function sleep(seconds) {
  console.log(`sleep ${seconds} seconds`);
  assert(Number.isInteger(seconds) && seconds > 0);
  return new Promise((resolve) => {
    setTimeout(resolve, seconds * 1000);
  });
}

module.exports = {
  toSQLDateString,
  sleep
};
