#[feature(struct_variant)];
#[feature(macro_rules)];

use osc::{OscType, OscMessage, OscWriter, OscReader};
use rpc::{ServerId, LogEntry, AppendEntriesRpc, AppendEntriesResponseRpc,
            RequestVoteRpc, RequestVoteResponseRpc, RaftRpc, AppendEntries,
            AppendEntriesResponse, RequestVote, RequestVoteResponse};
use std::io::net::ip::{Ipv4Addr, SocketAddr};
use std::io::net::udp::{UdpSocket, UdpStream};
use std::io::timer;
use std::os;
use std::vec;
use std::rand;
use transport::RaftRpcTransport;

mod rpc;
mod transport;

static raftPort : u16 = 9000;

pub struct UdpTransport {
  socket: UdpSocket,
  incomingRpcsChan: Chan<RaftRpc>,
  incomingRpcsPort: Port<RaftRpc>,
  outgoingRpcsChan: Chan<RaftRpc>,
  outgoingRpcsPort: Port<RaftRpc>,
}

impl UdpTransport {
  pub fn new() -> UdpTransport {
    let socket = UdpSocket::bind(SocketAddr {ip: Ipv4Addr(127,0,0,1), port:raftPort}).unwrap();
    let (incomingRpcsPort, incomingRpcsChan) = Chan::new();
    let (outgoingRpcsPort, outgoingRpcsChan) = Chan::new();

    return UdpTransport {socket: socket, incomingRpcsChan: incomingRpcsChan,
      incomingRpcsPort: incomingRpcsPort, outgoingRpcsChan: outgoingRpcsChan,
      outgoingRpcsPort: outgoingRpcsPort};
  }

  pub fn run(&self) {
    let readSocket = self.socket.clone();
    let writeSocket = self.socket.clone();

    //spawn(proc() {
    //  let mut udpStream = readSocket.connect(remoteAddr);
    //  loop {
    //    let msg = OscMessage::from_reader(&mut udpStream).unwrap();
    //    println!("recv {}: {:?}", msg.address, msg.arguments);
    //    let msgRpc = self.parseRpcMessage(msg);
    //    self.incomingRpcsChan.send(msgRpc);
    //  }
    //});

    //spawn(proc() {
    //  let mut udpStream = writeSocket.connect(remoteAddr);
    //  loop {
    //    let msgRpc = self.outgoingRpcsPort.recv();
    //    let msg = self.createRpcMessage(msgRpc);
    //    println!("send {}: {:?}", msg.address, msg.arguments);
    //    msg.write_to(&mut udpStream).unwrap();
    //  }
    //});
  }

  fn parseRpcMessage(&self, sender: ServerId, msg: OscMessage) -> RaftRpc {
    return match msg.address {
      ~"/appendEntries" => AppendEntries(self.parseAppendEntries(sender, msg.arguments)),
      ~"/requestVote" => RequestVote(self.parseRequestVote(sender, msg.arguments)),
      _ => fail!("woops no implementation for {}", msg.address)
    };
  }

  // AppendEntries {term: int, leaderId: ServerId, prevLogIndex: int,
  // entries: ~[LogEntry], leaderCommitIndex: int},
  fn parseAppendEntries(&self, sender: ServerId, argsVec: ~[OscType]) -> AppendEntriesRpc {
    let mut args = argsVec.move_iter();
    let term = args.next().unwrap().unwrap_int() as int;

    let leaderId: ServerId = from_str::<ServerId>(args.next().unwrap().unwrap_string()).unwrap();
    let prevLogIndex = args.next().unwrap().unwrap_int() as int;
    let prevLogTerm = args.next().unwrap().unwrap_int() as int;
    let entryCount = (args.len()-5)/2;
    let mut entries: ~[LogEntry] = vec::with_capacity(entryCount);
    for i in range(0,entryCount) {
      let term = args.next().unwrap().unwrap_int() as int;
      let entry = args.next().unwrap().unwrap_string();
      entries[i] = LogEntry {entry: entry, term: term};
    }
    let leaderCommitIndex = args.next().unwrap().unwrap_int() as int;
    return AppendEntriesRpc {sender: sender, term: term, leaderId: leaderId,
                          prevLogIndex: prevLogIndex, prevLogTerm: prevLogTerm,
                          entries: entries, leaderCommitIndex: leaderCommitIndex};
  }

  // RequestVote {term: int, candidateId: ServerId, lastLogIndex: int,
  // lastLogTerm: int}
  fn parseRequestVote(&self, sender: ServerId, argsVec: ~[OscType]) -> RequestVoteRpc {
    let mut args = argsVec.move_iter();
    let term: int = args.next().unwrap().unwrap_int() as int;
    let candidateId: ServerId = from_str::<ServerId>(args.next().unwrap().unwrap_string()).unwrap();
    let lastLogIndex: int = args.next().unwrap().unwrap_int() as int;
    let lastLogTerm: int = args.next().unwrap().unwrap_int() as int;
    return RequestVoteRpc {sender: sender, term: term, candidateId: candidateId,
                        lastLogIndex: lastLogIndex, lastLogTerm: lastLogTerm};
  }
}

impl RaftRpcTransport for UdpTransport {
  fn readIncoming(&self) -> Option<RaftRpc> {
    return Some(self.incomingRpcsPort.recv());
  }
  fn sendRpc(&self, recipient: ServerId, rpc: &RaftRpc) {
  }
}
