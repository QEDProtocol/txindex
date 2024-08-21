use std::{marker::PhantomData, sync::Arc};

use bitcoin::Block;
use kvq::{cache::KVQBinaryStoreCached, traits::KVQBinaryStoreImmutable};
use txindex_common::{db::{chain::TxIndexChainAPI, indexed_block::IndexedBlockFull, indexed_block_db::IndexedBlockDBStore, table::{core::KVQTableWrapper, traits::{get_real_key_at_block, KVQTableReaderAtBlock}}}, worker::traits::TxIndexWorker};

pub struct IndexForkHelper<KVQ: KVQBinaryStoreImmutable, T: TxIndexChainAPI, I: TxIndexWorker<KVQ, T>> {
    pub _db: PhantomData<I>,
    pub _kvq: PhantomData<KVQ>,
    pub _t: PhantomData<T>,
}


impl<KVQ: KVQBinaryStoreImmutable, T: TxIndexChainAPI, I: TxIndexWorker<KVQ, T>> IndexForkHelper<KVQ, T, I> {
  fn rollback_block(db: &mut IndexedBlockDBStore<KVQBinaryStoreCached<KVQ>>, block: &IndexedBlockFull) -> anyhow::Result<()> {
    let del_keys = block.added_fuzzy_block_keys.iter().chain(block.added_write_once_keys.iter()).chain(block.added_standard_keys.iter().map(|x|&x.key)).map(|key| key.to_vec()).collect::<Vec<Vec<u8>>>();
    db.store.store.imm_delete_many(&del_keys)?;
    block.modified_standard_keys.iter().map(|x| db.store.store.imm_set_ref(&x.key, &x.old_value)).collect::<anyhow::Result<()>>()?;
    db.store.store.imm_delete(&get_real_key_at_block::<IndexedBlockFull>(&block.metadata.block_number, 0x1fffffffffffffff)?)?;
    Ok(())
  }

  pub fn rollback_blocks(db: &mut IndexedBlockDBStore<KVQBinaryStoreCached<KVQ>>, incoming_block_number: u64) -> anyhow::Result<u64> {
    loop {
      let r = KVQTableWrapper::<IndexedBlockFull, KVQ>::get_leq_kv_at_block(&db.store.store, 0x1fffffffffffffff, &0x1fffffffffffffffu64, 0)?;
      if r.is_none() {
        return Ok(0);
      }
      let r = r.unwrap();
      let last_block_number = r.key;
      if r.key < incoming_block_number {
        return Ok(r.key+1);
      }
      Self::rollback_block(db, &r.value)?;
      if last_block_number == incoming_block_number {
        return Ok(last_block_number);
      }
    }
  }
  pub fn update_with_block(db: &mut IndexedBlockDBStore<KVQBinaryStoreCached<KVQ>>, q: Arc<T>, block_number: u64, block: &Block) -> anyhow::Result<()>{
    let ready_for_block_number = Self::rollback_blocks(db, block_number)?;
    if ready_for_block_number != block_number && block_number != 0 {
      for missing_block_num in ((ready_for_block_number..(block_number-1))) {
        let block = q.get_block(missing_block_num)?;
        I::process_block(db, Arc::clone(&q), missing_block_num, &block)?;
      }
    }
    I::process_block(db, q, block_number, block)?;
    Ok(())
  }
}