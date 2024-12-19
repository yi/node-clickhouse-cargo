
pkg = require "../package.json"
assert = require "assert"
http = require ('http')
https = require('https')

toSQLDateString = (date)->
  #debuglog "[toSQLDateString] date:", date
  return date.getUTCFullYear() + '-' +
    ('00' + (date.getUTCMonth()+1)).slice(-2) + '-' +
    ('00' + date.getUTCDate()).slice(-2) + ' ' +
    ('00' + date.getUTCHours()).slice(-2) + ':' +
    ('00' + date.getUTCMinutes()).slice(-2) + ':' +
    ('00' + date.getUTCSeconds()).slice(-2)


cargoOptionToHttpOption = (cargoOption, mixin)->
  options =
    #protocol : if cargoOption.vehicle is https then 'https:' else 'http:'
    host : cargoOption.host
    port : cargoOption.port
    headers :
      'X-ClickHouse-User' : cargoOption.user
      'X-ClickHouse-Key'  : cargoOption.password || ''
      'User-Agent' : "#{pkg.name}/#{pkg.version}"

  return Object.assign(mixin || {}, options)

sleep = (seconds)->
  console.log "sleep #{seconds} seconds"
  assert Number.isInteger(seconds) and seconds > 0
  return new Promise((resolve)=>
    setTimeout(resolve, seconds * 1000)
  )


module.exports =
  toSQLDateString : toSQLDateString
  cargoOptionToHttpOption : cargoOptionToHttpOption
  sleep : sleep

