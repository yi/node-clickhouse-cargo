# when in cluster worker mode, elect a leader for each cargo to avoid race condition when restoring existing bulks
assert = require "assert"
cluster = require('cluster')
dgram = require('dgram')
debuglog = require("debug")("chcargo:leader_election@#{if cluster.isMaster then "main" else cluster.worker.id}")

NOOP  = -> return

SERVICE_TYPE = "clickhouse-cargo"

SERVICE_PORT = 17888

SERVICE_HOST = '127.0.0.1'

REG_MSG_VALIDATOR = /^\d+@\w+/

MAX_UDP_CONFIRM = 10

RemoteClientPortByCargoId = {}

unless cluster.isMaster
  UDPEchoSrv = dgram.createSocket(type:'udp4')
  UDPEchoSrv.on 'message', (msg, rinfo)->
    msg = msg.toString('utf8')
    unless REG_MSG_VALIDATOR.test(msg)
      debuglog "[UDPEchoSrv] bad msg:#{msg}"
      return

    [workerId, cargoId] = msg.toString('utf8').split("@")
    {port} = rinfo
    #debuglog "[UDPEchoSrv > on msg] workerId:#{workerId}, cargoId:#{cargoId}, port:#{port}"

    portCollection = RemoteClientPortByCargoId[cargoId] || {}
    portCollection[port] = true
    RemoteClientPortByCargoId[cargoId] = portCollection

    # echo back
    for port of portCollection
      port = parseInt(port)
      UDPEchoSrv.send(msg, 0, msg.length, port, SERVICE_HOST) if port isnt SERVICE_PORT
    return

  UDPEchoSrv.on "error", (err)->
    debuglog "[static] UDPEchoSrv error:", err
    return

  try
    UDPEchoSrv.bind SERVICE_PORT, SERVICE_HOST, ->
      debuglog "[static] UDPEchoSrv bind SUCCESS"
      UDPEchoSrv.setBroadcast(true)
      return
  catch err
    debuglog "[static@#{cluster.worker.id}] UDPEchoSrv bind failed. error", err

# communicate with other cluster worker and to elect a leader worker for the given cargoId
electSelfToALeader = (cargoId, callbak=NOOP)->
  if cluster.isMaster and Object.keys(cluster.workers).length is 0
    debuglog "[electSelfToALeader] single process leader"
    callbak()
    return

  workerId = cluster.worker.id
  msg = Buffer.from(String(workerId) + "@" + cargoId)

  # broadcast self for a number of times
  countSend = 0
  cargoLeaderId = -1

  udpClient = dgram.createSocket("udp4")
  udpClient.on "message", (msg)->
    msg = msg.toString('utf8')
    unless REG_MSG_VALIDATOR.test(msg)
      debuglog "[udpClient] bad msg:#{msg}"
      return

    [remoteWorkerId, remoteCargoId] = msg.toString('utf8').split("@")
    #debuglog "[udpClient > on msg] workerId:#{remoteWorkerId}, cargoId:#{remoteCargoId}"
    unless remoteCargoId is cargoId
      debuglog "[udpClient] ignore non-interested remoteCargoId:#{remoteCargoId} as cargoId:#{cargoId}"
      return

    remoteWorkerId = parseInt(remoteWorkerId) || 0
    if remoteWorkerId > cargoLeaderId
      cargoLeaderId = remoteWorkerId
      debuglog "[udpClient] acknowledage new leader:#{remoteWorkerId} for #{cargoId}"
    return

  procSend = ->
    ++countSend
    if countSend > MAX_UDP_CONFIRM
      if cargoLeaderId is workerId
        debuglog "[electSelfToALeader@#{workerId}] is leader for #{cargoId}"
        callbak()
      else
        debuglog "[electSelfToALeader@#{workerId}] is follower for #{cargoId}"
      udpClient.close()
    else
      udpClient.send msg, 0, msg.length, SERVICE_PORT, SERVICE_HOST
      #setTimeout(procSend, Math.random() * 1000 >>> 0)
      setTimeout(procSend, 1000)
    return
  procSend()
  return


module.exports =
  electSelfToALeader : electSelfToALeader




