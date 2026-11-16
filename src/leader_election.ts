/**
 * Leader election module for cluster workers to avoid race conditions when
 * restoring existing bulks.
 *
 * Mechanism (all communication stays on 127.0.0.1:17888):
 *
 * - Every worker broadcasts a `${workerId}@cluster_leader_election` message
 *   over UDP with an exponential backoff that starts at 2ms and caps at 64ms.
 * - Every worker also tries to bind a UDP echo server on the shared port;
 *   exactly one worker wins the bind and becomes the echo hub. The losers
 *   simply keep a non-listening socket (bind errors are logged and ignored).
 * - The hub remembers the source port of every announcement it receives and
 *   re-broadcasts each message to all those ports.
 * - Every worker therefore hears every workerId (including its own echo) and
 *   keeps track of the highest id announced. The worker with the highest id
 *   is the leader and is the only one rotating files and committing to
 *   ClickHouse — see `Cargo.exam()`.
 *
 * Outside cluster mode (primary / standalone process) the process is always
 * the leader.
 */
import cluster from "cluster";
import dgram from "dgram";
import debug from "debug";

/** Identifies this process in log lines ("nocluster" when not running under cluster). */
const CLUSTER_WORKER_ID = cluster.isPrimary ? "nocluster" : String(cluster.worker!.id);
const debuglog = debug(`chcargo:leader_election@${CLUSTER_WORKER_ID}`);

const SERVICE_TYPE = "clickhouse-cargo";
/** Shared loopback UDP port used for announcements and echo relaying. */
const SERVICE_PORT = 17888;
const SERVICE_HOST = '127.0.0.1';
/** Valid messages look like `<workerId>@<cargoId>`, e.g. "3@cluster_leader_election". */
const REG_MSG_VALIDATOR = /^\d+@\w+/;
const MAX_UDP_CONFIRM = 10;

// (kept from the original implementation, currently unused)
const RemoteClientPortByCargoId: Record<string, number> = {};
/** workerId -> UDP source port of its announcement, maintained by the echo hub. */
const RemoteClientToPort: Record<string, number> = {};

/** Returns true when this process is the elected leader. */
let isThisLeader: () => boolean;
/** Returns the worker id currently believed to be the leader. */
let getLeaderId: () => number;

if (cluster.isPrimary) {
  // Primary / standalone process is always the leader.
  isThisLeader = () => true;
  getLeaderId = () => 0;
} else {
  // When running as a cluster
  /** Highest worker id this worker has ever heard of; the leader is this id. */
  let KnownHighestWorkerId = cluster.worker!.id;

  // Setup UDP server: only one worker manages to bind the shared port and
  // thus becomes the echo hub for everyone else.
  const UDPEchoSrv = dgram.createSocket({
    type: 'udp4'
  });

  UDPEchoSrv.on('message', (msg, rinfo) => {
    const text = msg.toString('utf8');
    if (!REG_MSG_VALIDATOR.test(text)) {
      debuglog(`[UDPEchoSrv] bad msg:${text}`);
      return;
    }

    const [workerId] = text.split("@");
    const { port } = rinfo;

    RemoteClientToPort[workerId] = port;

    // Echo back: relay the announcement to every worker we know about,
    // skipping the hub's own service port.
    for (const [workerId, port] of Object.entries(RemoteClientToPort)) {
      if (port !== SERVICE_PORT) {
        UDPEchoSrv.send(text, 0, text.length, port, SERVICE_HOST);
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
    debuglog(`[static@${cluster.worker!.id}] UDPEchoSrv bind failed. error`, err);
  }

  // Setup UDP client: broadcast our own announcement and track the highest
  // worker id among all announcements echoed back by the hub.
  const udpClient = dgram.createSocket("udp4");

  udpClient.on("message", (msg) => {
    const text = msg.toString('utf8');
    if (!REG_MSG_VALIDATOR.test(text)) {
      debuglog(`[udpClient] bad msg:${text}`);
      return;
    }

    const [remoteWorkerId] = text.split("@");
    const remoteWorkerIdNum = parseInt(remoteWorkerId) || 0;

    if (remoteWorkerIdNum > KnownHighestWorkerId) {
      debuglog(`[udpClient] acknowledage new leader: ${KnownHighestWorkerId} -> ${remoteWorkerIdNum}`);
      KnownHighestWorkerId = remoteWorkerIdNum;
    }
  });

  let ClientSentCount = 2;
  const msg = Buffer.from(`${cluster.worker!.id}@cluster_leader_election`);

  // Send our announcement, then reschedule with a doubling interval (2..64ms).
  const procSend = () => {
    if (!(ClientSentCount >= 64)) {
      ClientSentCount *= 2;
    }
    udpClient.send(msg, 0, msg.length, SERVICE_PORT, SERVICE_HOST);
    setTimeout(procSend, ClientSentCount);
  };

  procSend();

  isThisLeader = () => KnownHighestWorkerId === cluster.worker!.id;
  getLeaderId = () => KnownHighestWorkerId;
}

export {
  isThisLeader,
  getLeaderId
};
