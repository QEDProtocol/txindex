use bitcoin::Transaction;

use crate::chain::Network;

pub trait TxIndexChainAPI {
  fn get_transaction(&self, txid: [u8; 32]) -> anyhow::Result<Transaction>;
  fn get_block(&self, block_number: u64) -> anyhow::Result<bitcoin::Block>;
  fn get_blockhash(&self, block_number: u64) -> anyhow::Result<bitcoin::BlockHash>;
  fn get_block_by_hash(&self, hash: [u8; 32]) -> anyhow::Result<bitcoin::Block>;
  fn get_latest_block(&self) -> anyhow::Result<bitcoin::Block>;
  fn get_network(&self) -> Network;
  fn get_bitcoin_network(&self) -> bitcoin::Network;
}