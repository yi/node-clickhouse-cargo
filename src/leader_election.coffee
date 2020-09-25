# when in cluster worker mode, elect a leader for each cargo
bonjour = require('bonjour')()
assert = require "assert"
cluster = require('cluster')
debuglog = require("debug")("chcargo:leader_election")

NOOP  = -> return

SERVICE_TYPE = "clickhouse-cargo"

# https://stackoverflow.com/a/15075395/305945
getLocalIPAddress = ->
  interfaces = require('os').networkInterfaces()
  for devName of interfaces
    iface = interfaces[devName]
    i = 0
    while i < iface.length
      alias = iface[i]
      if alias.family == 'IPv4' and alias.address != '127.0.0.1' and !alias.internal
        return alias.address
      i++
  return '0.0.0.0'

electSelfToALeader = (cargoId, callbak=NOOP)->
  cargoId = String(cargoId || '').trim()
  assert cargoId, "missing cargoId"

  if cluster.isMaster and Object.keys(cluster.workers).length is 0
    debuglog "[electSelfToALeader] single process leader"
    callbak()
    return

  commonOptions =
    #host : getLocalIPAddress()
    protocol : 'udp'
    type : SERVICE_TYPE + cargoId
    port : 17888

  #options = Object.assign(txt : workerId : cluster.worker.id, commonOptions)
  options = Object.assign(name : String(cluster.worker.id), commonOptions)
  debuglog "[electSelfToALeader] options:", options
  bonjour.publish(options)

  acknowledagedServices = []
  bonjour.find commonOptions, (service)->
    debuglog "[electSelfToALeader@#{cluster.worker.id}] add service:", service
    acknowledagedServices.push(service)
    return

  detect = ->
    debuglog "[electSelfToALeader@#{cluster.worker.id} > detect] acknowledagedServices:"
    console.dir acknowledagedServices, depth:10
    return

  #detect = ->
    #debuglog "[electSelfToALeader > detect]"
    #bonjour.find commonOptions, (services)->
      #debuglog "[electSelfToALeader > find] services"
      #console.dir(services, depth:10)

      #debuglog "[electSelfToALeader] services"
      #return
    #return

  setTimeout(detect, 60000)
  return


module.exports =
  electSelfToALeader : electSelfToALeader




