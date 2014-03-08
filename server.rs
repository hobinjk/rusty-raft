#[feature(struct_variant)];
#[feature(macro_rules)];

use std::io::net::ip::{Ipv4Addr, SocketAddr};
use std::io::net::udp::{UdpSocket, UdpStream};
use std::io::timer;
use udptransport::UdpTransport;
use transport::RaftRpcTransport;
use rpc::{ServerId, LogEntry, AppendEntries, AppendEntriesResponse, RequestVote, RequestVoteResponse, RaftRpc};
use rpc::{AppendEntriesRpc, AppendEntriesResponseRpc, RequestVoteRpc, RequestVoteResponseRpc};
use std::os;
use std::vec;
use std::rand;

#[path="./rust-osc/osc.rs"]
mod osc;
mod udptransport;
mod transport;
mod rpc;

enum ServerType {
  Follower,
  Candidate,
  Leader
}

struct RaftServer {
  currentTerm: int,
  votedFor: Option<ServerId>,
  log: ~[LogEntry],
  commitIndex: int,
  lastApplied: int,
  serverType: ServerType,
  electionTimeout: int,
  receivedVotes: int,
  // Leader state:
  // for each server, index of the next log entry to send to that
  // server, initialized to last log index + 1
  nextIndex: ~[int],
  // for each server, index of highest log entry known to be
  // replicated on server, initialized to 0
  matchIndex: ~[int],
  // current set of servers
  servers: ~[ServerId],
  // serverId corresponding to self
  serverId: ServerId,
  // transport layer to send RPC's over
  transport: ~RaftRpcTransport
}

impl RaftServer {
  fn new(transport: ~RaftRpcTransport, serverId: ServerId, servers: ~[ServerId]) -> RaftServer {
    return RaftServer {
      currentTerm: 0,
      votedFor: None,
      log: ~[],
      commitIndex: 0,
      lastApplied: 0,
      electionTimeout: 0,
      receivedVotes: 0,
      serverType: Follower,
      nextIndex:  vec::with_capacity(servers.len()),
      matchIndex: vec::with_capacity(servers.len()),
      servers: servers,
      serverId: serverId,
      transport: transport
    }
  }

  fn run(&mut self) {
    loop {
      match self.serverType {
        Candidate => self.candidateStep(),
        Follower => self.followerStep(),
        Leader => self.leaderStep()
      }
    }
  }

  // Act as a candidate
  // if votes received from a majority of servers become leader
  // if appendentries received from new leader convert to follower
  // if election times out try again
  fn candidateStep(&mut self) {
    match self.transport.readIncoming() {
      Some(rpc) => self.candidateRespond(rpc),
      None => {}
    }

    if self.receivedVotes > (self.servers.len()/2) as int {
      self.convertToLeader();
    }
  }

  // Respond as a candidate to a given RPC
  // RequestVoteResponse with success means we get a vote :D
  // AppendEntries with term >= our term means we lost T_T
  fn candidateRespond(&mut self, rpc: RaftRpc) {
    match rpc {
      RequestVoteResponse(rvr) => self.candidateRequestVoteResponse(rvr),
      AppendEntries(ae) => self.candidateAppendEntries(ae),
      _ => {}
    };
  }

  fn candidateRequestVoteResponse(&mut self, rpc: RequestVoteResponseRpc) {
    if rpc.voteGranted {
      // TODO check to see if the server already voted for us this cycle
      self.receivedVotes += 1;
    }
  }

  fn candidateAppendEntries(&mut self, rpc: AppendEntriesRpc) {
    if rpc.term >= self.currentTerm {
      // we lost the election... D:
      self.convertToFollower();
    }
    // pretend we didn't hear them whether or not they won, the resend will occur anyway
  }


  // Update the server when it is a Follower
  // Paper:
  //  Respond to RPCs from candidates and leaders
  //  If election timeout elapses without receiving AppendEntries RPC
  //  or granting vote to candidate: convert to candidate
  fn followerStep(&mut self) {
    match self.transport.readIncoming() {
      Some(rpc) => self.followerRespond(rpc),
      None => {}
    }

    self.electionTimeout -= 1;
    if self.electionTimeout < 0 {
      self.convertToCandidate();
    }
  }

  // Respond to an incoming RPC as a follower
  fn followerRespond(&mut self, rpc: RaftRpc) {
    let response = match rpc {
      AppendEntries(ref ae) => Some(self.followerAppendEntries(ae)),
      RequestVote(ref rv) => Some(self.followerRequestVote(rv)),
      _ => None
    };

    match response {
      // send response to original rpc sender
      Some(responseRpc) => self.transport.sendRpc(rpc.sender(), &responseRpc),
      None => {}
    }
  }

  // As a follower, handle an appendEntries RPC
  fn followerAppendEntries(&mut self, rpc: &AppendEntriesRpc) -> RaftRpc {
    let fail = AppendEntriesResponse(AppendEntriesResponseRpc{sender: self.serverId, term: self.currentTerm, success: false, logIndex: 0});

    if rpc.term < self.currentTerm {
      return fail;
    }
    // If log doesn't contain an entry with matching term return false
    if rpc.prevLogIndex < self.log.len() as int {
      if self.log[rpc.prevLogIndex].term != rpc.prevLogTerm {
        return fail;
      }
    } else {
      return fail;
    }
    // 3. If an existing entry conflicts with a new one delete the
    //    existing entry and all that follow it
    let startLogIndex = rpc.prevLogIndex+1;
    for logOffset in range(0, rpc.entries.len()) {
      let logIndex = startLogIndex + logOffset as int;
      let entry = rpc.entries[logOffset].clone();
      if logIndex < self.log.len() as int {
        if self.log[logIndex].term != entry.term {
          // delete it and all following
          self.log.truncate(logIndex as uint);
          self.log.push(entry);
        }
      } else {
        self.log.push(entry);
      }
    }

    return AppendEntriesResponse(AppendEntriesResponseRpc {
      sender: self.serverId, term: self.currentTerm, success: true,
      logIndex: (self.log.len() - 1) as int
    });
  }

  // As a follower handle a requestVote rpc
  // From paper:
  // 1. Reply false if term < currentTerm
  // 2. If votedFor is null or candidateId and candidate's log is
  // at least as up-to-date as receiver's log, grant vote
  fn followerRequestVote(&mut self, rpc: &RequestVoteRpc) -> RaftRpc {
    let fail = RequestVoteResponse(RequestVoteResponseRpc {sender: self.serverId, term: self.currentTerm, voteGranted: false});
    if rpc.term < self.currentTerm {
      return fail;
    }
    // if we haven't voted for anything or we voted for candidate
    match self.votedFor {
      None => {
        return self.followerVote(rpc);
      },
      Some(id) if rpc.candidateId == id => {
        return self.followerVote(rpc);
      }
      _ => {
        return fail;
      }
    }
  }

  fn followerVote(&mut self, rpc: &RequestVoteRpc) -> RaftRpc {
    // if the candidate's log is at least as up-to-date as ours vote for them
    let mut voteGranted = false;
    let lastLogIndex = (self.log.len() - 1) as int;
    if self.log.len() == 0 || (rpc.lastLogIndex >= lastLogIndex &&
                                 rpc.lastLogTerm >= self.log[lastLogIndex].term) {
      self.votedFor = Some(rpc.candidateId);
      voteGranted = true
    }
    return RequestVoteResponse(RequestVoteResponseRpc {sender: self.serverId, term: self.currentTerm, voteGranted: voteGranted});
  }


  // Update as a leader
  // Paper:
  // If last log index > nextIndex for a follower send AppendEntries RPC with log entries starting at nextIndex
  // If a successful appendEntries is received update nextIndex and matchIndex of follower
  // Otherwise decrement nextIndex of follower and retry
  // If there exists an N such that N > commitIndex, a majority of matchIndex >= N and log[N].term == currentTerm
  // set commitIndex = N
  fn leaderStep(&mut self) {
    match self.transport.readIncoming() {
      Some(rpc) => self.leaderRespond(rpc),
      None => {}
    }
  }

  fn leaderRespond(&mut self, rpc: RaftRpc) {
    match rpc {
      AppendEntriesResponse(aer) => self.leaderAppendEntriesResponse(aer),
      _ => {}
    }
  }

  // If a successful appendEntries is received update nextIndex and matchIndex of follower
  // Otherwise decrement nextIndex of follower and retry
  fn leaderAppendEntriesResponse(&mut self, rpc: AppendEntriesResponseRpc) {
    let followerIndex = self.getServerIndex(rpc.sender);
    if rpc.success {
      self.nextIndex[followerIndex] = rpc.logIndex;
      self.matchIndex[followerIndex] = rpc.logIndex;
    } else {
      if self.nextIndex[followerIndex] > 0 {
        self.nextIndex[followerIndex] -= 1;
      }
    }
  }

  // Become a candidate (start election)
  fn convertToCandidate(&mut self) {
    self.serverType = Candidate;
    self.currentTerm += 1;
    self.receivedVotes = 1; // vote for self
    self.setNewTimeout();

    // RequestVote {sender: ServerId, term: int, candidateId: ServerId, lastLogIndex: int, lastLogTerm: int},
    let lastLogIndex = (self.log.len() - 1) as int;
    let requestVote = RequestVote(RequestVoteRpc {
        sender: self.serverId, term: self.currentTerm,
        candidateId: self.serverId, lastLogIndex: lastLogIndex,
        lastLogTerm: self.log[lastLogIndex].term
    });
    // Broadcast requestVote to all servers
    self.broadcastRpc(requestVote);
  }

  fn convertToFollower(&mut self) {
    self.serverType = Follower;
    self.setNewTimeout();
  }

  fn convertToLeader(&mut self) {
    self.serverType = Leader;
    self.broadcastHeartbeat();
  }

  fn broadcastRpc(&mut self, rpc: RaftRpc) {
    for &serverId in self.servers.iter() {
      if serverId == self.serverId {
        continue;
      }
      self.transport.sendRpc(serverId, &rpc);
    }
  }

  fn broadcastHeartbeat(&mut self) {
    // Send an empty appendEntries to all servers
    // AppendEntries {sender: ServerId, term: int, leaderId: ServerId, prevLogIndex: int, entries: ~[LogEntry], leaderCommitIndex: int},
    let lastLogIndex = (self.log.len() - 1) as int;
    let appendEntries = AppendEntries(AppendEntriesRpc {
      sender: self.serverId, term: self.currentTerm, leaderId: self.serverId,
      prevLogIndex: lastLogIndex, prevLogTerm: self.log[lastLogIndex].term,
      entries: ~[], leaderCommitIndex: lastLogIndex
    });
    self.broadcastRpc(appendEntries);
  }

  fn getServerIndex(&self, serverId: ServerId) -> int {
    for i in range(0,self.servers.len() as int) {
      if self.servers[i] == serverId {
        return i;
      }
    }
    return -1;
  }

  fn setNewTimeout(&mut self) {
    let val:int = rand::random();
    self.electionTimeout = val % 500 + 500;
  }
}

fn getLocalAddr(port: u16) -> SocketAddr {
  return SocketAddr {ip: Ipv4Addr(127,0,0,1), port: port};
}

fn main() {
  let args = os::args();
  if args.len() != 2 {
    fail!("usage: {} <port>", args[0]);
  }

  let port = match from_str::<int>(args[1]) {
    Some(val) => val,
    None => fail!("usage: {} <port:int>", args[0])
  };

  let udpTransport = UdpTransport::new();
  // config with 5 servers
  let servers = vec::from_fn(5, |idx| getLocalAddr(9000 + idx as u16));
  let mut server = RaftServer::new(~udpTransport, getLocalAddr(port as u16), servers);
  server.run();
}

