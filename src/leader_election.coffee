# when in cluster worker mode, elect a leader for each cargo to avoid race condition when restoring existing bulks
assert = require "assert"
cluster = require('cluster')
debuglog = require("debug")("chcargo:leader_election")
dgram = require('dgram')

NOOP  = -> return

SERVICE_TYPE = "clickhouse-cargo"

SERVICE_PORT = 17888

SERVICE_HOST = '127.0.0.1'

UDPSock = null

CargoIdToLeadWorkerId = {}

initUDPSock = ->
  return if UDPSock

  UDPSock = dgram.createSocket({type:'udp4', reuseAddr:true })
  UDPSock.on 'message', (msg, rinfo)->
    msg = msg.toString('utf8')
    debuglog "[on msg@#{cluster.worker.id}] msg:", msg, ", rinfo:", rinfo
    [workerId, cargoId] = msg.toString('utf8').split("@")
    workerId = parseInt(workerId) || 0
    unless workerId > 0 and cargoId
      debuglog "[sock:message@#{cluster.worker.id}] bad msg:#{msg} or cargoId:#{cargoId}"
      return

    acknowledagedCargoLeaderId = parseInt(CargoIdToLeadWorkerId[cargoId]) || 0
    # make highest cluster.worker.id as the cargo leader
    CargoIdToLeadWorkerId[cargoId] = workerId if workerId > acknowledagedCargoLeaderId
    debuglog "[on msg@#{cluster.worker.id}] CargoIdToLeadWorkerId:", CargoIdToLeadWorkerId
    return

  UDPSock.bind SERVICE_PORT, SERVICE_HOST, -> UDPSock.setBroadcast(true)
  return

electSelfToALeader = (cargoId, callbak=NOOP)->

  if cluster.isMaster and Object.keys(cluster.workers).length is 0
    debuglog "[electSelfToALeader] single process leader"
    callbak()
    return

  initUDPSock()

  msg = Buffer.from(String(cluster.worker.id) + "@" + cargoId)

  # broadcast self for a number of times
  countSend = 0
  procSend = ->
    ++countSend
    if countSend < 100
      UDPSock.send msg, 0, msg.length, SERVICE_PORT, SERVICE_HOST
      setTimeout(procSend, Math.random() * 1000 >>> 0)
    else
      workerId = cluster.worker.id
      if CargoIdToLeadWorkerId[cargoId] is workerId
        debuglog "[electSelfToALeader@#{workerId}] is leader"
        callbak()
      else
        debuglog "[electSelfToALeader@#{workerId}] is follower"

    return
  procSend()

  return


module.exports =
  electSelfToALeader : electSelfToALeader




