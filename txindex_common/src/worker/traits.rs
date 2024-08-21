
use std::sync::Arc;

use bitcoin::Block;
use kvq::{cache::KVQBinaryStoreCached, traits::KVQBinaryStoreImmutable};

use crate::db::{chain::TxIndexChainAPI, indexed_block_db::IndexedBlockDBStore};
pub trait TxIndexWorker<KVQ: KVQBinaryStoreImmutable, T: TxIndexChainAPI> {
  fn process_block(db: &mut IndexedBlockDBStore<KVQBinaryStoreCached<KVQ>>, q: Arc<T>, block_number: u64, block: &Block) -> anyhow::Result<()>;
}