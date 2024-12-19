// Leader election module for cluster workers to avoid race conditions when restoring existing bulks
const cluster = require('cluster');
const dgram = require('dgram');

const CLUSTER_WORKER_ID = cluster.isPrimary ? "nocluster" : cluster.worker.id;
const debuglog = require("debug")(`chcargo:leader_election@${CLUSTER_WORKER_ID}`);

const SERVICE_TYPE = "clickhouse-cargo";
const SERVICE_PORT = 17888;
const SERVICE_HOST = '127.0.0.1';
const REG_MSG_VALIDATOR = /^\d+@\w+/;
const MAX_UDP_CONFIRM = 10;

const RemoteClientPortByCargoId = {};
const RemoteClientToPort = {};

let isThisLeader;
let getLeaderId;

if (cluster.isPrimary) {
  isThisLeader = () => true;
  getLeaderId = () => 0;
} else {
  // When running as a cluster
  let KnownHighestWorkerId = cluster.worker.id;

  // Setup UDP server
  const UDPEchoSrv = dgram.createSocket({
    type: 'udp4'
  });

  UDPEchoSrv.on('message', (msg, rinfo) => {
    msg = msg.toString('utf8');
    if (!REG_MSG_VALIDATOR.test(msg)) {
      debuglog(`[UDPEchoSrv] bad msg:${msg}`);
      return;
    }

    const [workerId, cargoId] = msg.toString('utf8').split("@");
    const { port } = rinfo;

    RemoteClientToPort[workerId] = port;

    // Echo back
    for (const [workerId, port] of Object.entries(RemoteClientToPort)) {
      const portNum = parseInt(port);
      if (portNum !== SERVICE_PORT) {
        UDPEchoSrv.send(msg, 0, msg.length, portNum, SERVICE_HOST);
      }
    }
  });

  UDPEchoSrv.on("error", (err) => {
    debuglog("[static] UDP_ERR UDPEchoSrv error:", err);
  });

  try {
    UDPEchoSrv.bind(SERVICE_PORT, SERVICE_HOST, () => {
      debuglog("[static] UDPEchoSrv bind SUCCESS");
      UDPEchoSrv.setBroadcast(true);
    });
  } catch (err) {
    debuglog(`[static@${cluster.worker.id}] UDPEchoSrv bind failed. error`, err);
  }

  // Setup UDP client
  const udpClient = dgram.createSocket("udp4");

  udpClient.on("message", (msg) => {
    msg = msg.toString('utf8');
    if (!REG_MSG_VALIDATOR.test(msg)) {
      debuglog(`[udpClient] bad msg:${msg}`);
      return;
    }

    const [remoteWorkerId, remoteCargoId] = msg.toString('utf8').split("@");
    const remoteWorkerIdNum = parseInt(remoteWorkerId) || 0;

    if (remoteWorkerIdNum > KnownHighestWorkerId) {
      debuglog(`[udpClient] acknowledage new leader: ${KnownHighestWorkerId} -> ${remoteWorkerIdNum}`);
      KnownHighestWorkerId = remoteWorkerIdNum;
    }
  });

  let ClientSentCount = 2;
  const msg = Buffer.from(`${cluster.worker.id}@cluster_leader_election`);

  const procSend = () => {
    if (!(ClientSentCount >= 64)) {
      ClientSentCount *= 2;
    }
    udpClient.send(msg, 0, msg.length, SERVICE_PORT, SERVICE_HOST);
    setTimeout(procSend, ClientSentCount);
  };

  procSend();

  isThisLeader = () => KnownHighestWorkerId === cluster.worker.id;
  getLeaderId = () => KnownHighestWorkerId;
}

module.exports = {
  isThisLeader,
  getLeaderId
};
