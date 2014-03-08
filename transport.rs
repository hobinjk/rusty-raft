use rpc::{ServerId, RaftRpc};
mod rpc;

pub trait RaftRpcTransport {
  fn sendRpc(&self, client: ServerId, rpc: &RaftRpc);
  fn readIncoming(&self) -> Option<RaftRpc>;
}

