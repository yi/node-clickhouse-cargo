const pkg = require("../package.json");
const assert = require("assert");
const http = require('http');
const https = require('https');

function toSQLDateString(date) {
  //debuglog "[toSQLDateString] date:", date
  return date.getUTCFullYear() + '-' +
    ('00' + (date.getUTCMonth() + 1)).slice(-2) + '-' +
    ('00' + date.getUTCDate()).slice(-2) + ' ' +
    ('00' + date.getUTCHours()).slice(-2) + ':' +
    ('00' + date.getUTCMinutes()).slice(-2) + ':' +
    ('00' + date.getUTCSeconds()).slice(-2);
}

function cargoOptionToHttpOption(cargoOption, mixin) {
  const options = {
    //protocol : if cargoOption.vehicle is https then 'https:' else 'http:'
    host: cargoOption.host,
    port: cargoOption.port,
    headers: {
      'X-ClickHouse-User': cargoOption.user,
      'X-ClickHouse-Key': cargoOption.password || '',
      'User-Agent': `${pkg.name}/${pkg.version}`
    }
  };
  return Object.assign(mixin || {}, options);
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
  cargoOptionToHttpOption,
  sleep
};
