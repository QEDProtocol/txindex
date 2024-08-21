use std::{borrow::BorrowMut, sync::Arc};

use bitcoin::Block;
use kvq::{cache::KVQBinaryStoreCachedTrait, traits::{KVQBinaryStoreReader, KVQPair}};


use super::{indexed_block::{IndexedBlockFull, IndexedBlockMetadata, SerializedIndexedBlockAction}, table::{core::{KVQTable, KVQTableWrapper, TABLE_TYPE_FUZZY_BLOCK_INDEX}, traits::{KVQTableReaderAtBlock, KVQTableWriterAtBlock}}};

#[derive(Debug, Clone)]
pub struct IndexedBlockDBStore<S: KVQBinaryStoreCachedTrait> {
  pub store: S,
  pub block_number: u64,
  pub metadata: IndexedBlockMetadata,
  pub actions: Vec<SerializedIndexedBlockAction>,
}

impl<S: KVQBinaryStoreCachedTrait> IndexedBlockDBStore<S> {
  pub fn prev_block_number(&self) -> u64 {
    if self.block_number == 0 {
      0
    } else {
      self.block_number - 1
    }
  }
  pub fn new_from_block(store: S, block_number: u64, block: &Block) -> Self {
    Self {
      store,
      block_number,
      metadata: IndexedBlockMetadata::new_from_block(block_number, block),
      actions: Vec::new(),
    }
  }

  pub fn get<T: KVQTable>(&self, key: &T::Key) -> anyhow::Result<Option<T::Value>> {
    if T::TABLE_TYPE == TABLE_TYPE_FUZZY_BLOCK_INDEX {
      KVQTableWrapper::<T, S>::get_leq_at_block(&self.store, self.block_number, key, 0)
    }else{
      KVQTableWrapper::<T, S>::get_exact_if_exists_at_block(&self.store, self.block_number, key)
    }
  }
  pub fn put<T: KVQTable>(&mut self, key: &T::Key, value: &T::Value) -> anyhow::Result<()> {
    KVQTableWrapper::<T, S>::set_ref_at_block(self.store.borrow_mut(), self.block_number, key, value)
  }
  pub fn put_many_ref<T: KVQTable>(&mut self, items: &[KVQPair<&T::Key, &T::Value>]) -> anyhow::Result<()> {
    KVQTableWrapper::<T, S>::set_many_ref_at_block(self.store.borrow_mut(), self.block_number, items)
  }
  pub fn put_many<T: KVQTable>(&mut self, items: &[KVQPair<T::Key, T::Value>]) -> anyhow::Result<()> {
    KVQTableWrapper::<T, S>::set_many_at_block(self.store.borrow_mut(), self.block_number, items)
  }
  pub fn get_latest_synced_block(&self) -> anyhow::Result<u64> {
    let r = KVQTableWrapper::<IndexedBlockFull, S>::get_leq_kv_at_block(&self.store, 0x1fffffffffffffff, &0x1fffffffffffffffu64, 0)?;
    if let Some(kv) = r {
      Ok(kv.key)
    } else {
      Ok(0xffffffffffffffff)
    }
  }
}


#[derive(Debug, Clone)]
pub struct IndexedBlockDBStoreReader<S: KVQBinaryStoreReader> {
  pub store: Arc<S>,
}

impl<S: KVQBinaryStoreReader> IndexedBlockDBStoreReader<S> {
  pub fn new_from_block(store: S) -> Self {
    Self {
      store: Arc::new(store),
    }
  }

  pub fn get<T: KVQTable>(&self, key: &T::Key) -> anyhow::Result<Option<T::Value>> {
    if T::TABLE_TYPE == TABLE_TYPE_FUZZY_BLOCK_INDEX {
      KVQTableWrapper::<T, S>::get_leq_at_block(&self.store, 0xffffffffffffffff, key, 0)
    }else{
      KVQTableWrapper::<T, S>::get_exact_if_exists_at_block(&self.store, 0xffffffffffffffff, key)
    }
  }
}