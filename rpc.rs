use std::io::net::ip::SocketAddr;

pub type ServerId = SocketAddr;

#[deriving(Clone)]
pub struct LogEntry {
  entry: ~str,
  term: int
}

pub struct AppendEntriesRpc {
  sender: ServerId,
  term: int,
  leaderId: ServerId,
  prevLogIndex: int,
  prevLogTerm: int,
  entries: ~[LogEntry],
  leaderCommitIndex: int
}

pub struct AppendEntriesResponseRpc {
  sender: ServerId,
  term: int,
  success: bool,
  logIndex: int
}

pub struct RequestVoteRpc {
  sender: ServerId,
  term: int,
  candidateId: ServerId,
  lastLogIndex: int,
  lastLogTerm: int
}

pub struct RequestVoteResponseRpc {
  sender: ServerId,
  term: int,
  voteGranted: bool
}

pub enum RaftRpc {
  AppendEntries(AppendEntriesRpc),
  AppendEntriesResponse(AppendEntriesResponseRpc),
  RequestVote(RequestVoteRpc),
  RequestVoteResponse(RequestVoteResponseRpc)
}

impl RaftRpc {
  pub fn sender(self) -> ServerId {
    match self {
      AppendEntries(rpc) => rpc.sender,
      AppendEntriesResponse(rpc) => rpc.sender,
      RequestVote(rpc) => rpc.sender,
      RequestVoteResponse(rpc) => rpc.sender
    }
  }
}

