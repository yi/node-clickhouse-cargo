# when in cluster worker mode, elect a leader for each cargo to avoid race condition when restoring existing bulks
assert = require "assert"
cluster = require('cluster')
dgram = require('dgram')
CLUSTER_WORKER_ID = if cluster.isMaster then "nocluster" else cluster.worker.id
debuglog = require("debug")("chcargo:leader_election@#{CLUSTER_WORKER_ID}")

SERVICE_TYPE = "clickhouse-cargo"

SERVICE_PORT = 17888

SERVICE_HOST = '127.0.0.1'

REG_MSG_VALIDATOR = /^\d+@\w+/

MAX_UDP_CONFIRM = 10

RemoteClientPortByCargoId = {}

RemoteClientToPort = {}



if cluster.isMaster
  isThisLeader = -> return true
  getLeaderId = -> return 0

else
  # when running as a cluster
  KnownHighestWorkerId = cluster.worker.id

  # setup upd server
  UDPEchoSrv = dgram.createSocket(type:'udp4')
  UDPEchoSrv.on 'message', (msg, rinfo)->
    msg = msg.toString('utf8')
    unless REG_MSG_VALIDATOR.test(msg)
      debuglog "[UDPEchoSrv] bad msg:#{msg}"
      return

    [workerId, cargoId] = msg.toString('utf8').split("@")
    {port} = rinfo
    #debuglog "[UDPEchoSrv > on msg] workerId:#{workerId}, cargoId:#{cargoId}, port:#{port}"

    RemoteClientToPort[workerId] = port

    #debuglog "[UDPEchoSrv > on msg] cargoId:#{cargoId}, RemoteClientToPort:", RemoteClientToPort
    # echo back
    for workerId, port of RemoteClientToPort
      port = parseInt(port)
      if port isnt SERVICE_PORT
        UDPEchoSrv.send(msg, 0, msg.length, port, SERVICE_HOST)
        #debuglog "[UDPEchoSrv] echo back to worker:#{workerId}@port:#{port}"
    return

  UDPEchoSrv.on "error", (err)->
    debuglog "[static] UDP_ERR UDPEchoSrv error:", err
    return

  try
    UDPEchoSrv.bind SERVICE_PORT, SERVICE_HOST, ->
      debuglog "[static] UDPEchoSrv bind SUCCESS"
      UDPEchoSrv.setBroadcast(true)
      return
  catch err
    debuglog "[static@#{cluster.worker.id}] UDPEchoSrv bind failed. error", err

  # setup udp client
  udpClient = dgram.createSocket("udp4")
  udpClient.on "message", (msg)->
    msg = msg.toString('utf8')
    unless REG_MSG_VALIDATOR.test(msg)
      debuglog "[udpClient] bad msg:#{msg}"
      return

    [remoteWorkerId, remoteCargoId] = msg.toString('utf8').split("@")

    #debuglog "[udpClient] on msg remoteWorkerId:#{remoteWorkerId}, remoteCargoId:#{remoteCargoId}"
    remoteWorkerId = parseInt(remoteWorkerId) || 0

    if remoteWorkerId > KnownHighestWorkerId
      debuglog "[udpClient] acknowledage new leader: #{KnownHighestWorkerId} -> #{remoteWorkerId}"
      KnownHighestWorkerId = remoteWorkerId
    return

  ClientSentCount = 2
  msg = Buffer.from("#{cluster.worker.id}@cluster_leader_election")

  procSend = ->
    ClientSentCount *= 2 unless ClientSentCount >= 64
    udpClient.send msg, 0, msg.length, SERVICE_PORT, SERVICE_HOST
    setTimeout(procSend, ClientSentCount)
    return
  procSend()

  isThisLeader = -> return KnownHighestWorkerId is cluster.worker.id
  getLeaderId = -> return KnownHighestWorkerId

module.exports =
  isThisLeader : isThisLeader
  getLeaderId : getLeaderId



