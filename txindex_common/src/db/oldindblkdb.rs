/*use std::{borrow::BorrowMut, sync::Arc};

use bitcoin::Block;
use kvq::{cache::{KVQBinaryStoreCached, KVQBinaryStoreCachedTrait}, traits::{KVQBinaryStore, KVQPair, KVQSerializable}};

use crate::db::table::{core::TABLE_TYPE_STANDARD, traits::get_real_key_at_block};

use super::{indexed_block::{IndexedBlockFull, IndexedBlockMetadata}, table::{core::{KVQTable, KVQTableWrapper, TABLE_TYPE_FUZZY_BLOCK_INDEX, TABLE_TYPE_WRITE_ONCE}, traits::{KVQTableReaderAtBlock, KVQTableWriterAtBlock}}};

#[derive(Debug, Clone)]
pub struct IndexedBlockDBStore<S: KVQBinaryStoreCachedTrait> {
  pub store: S,
  pub block_number: u64,
  pub indexed_block: IndexedBlockFull,
  pub modified_standard_keys_list: Vec<SerializedModifiedStandardKey>,
  pub added_standard_keys_list: Vec<SerializedModifiedStandardKey>,
}

impl<S: KVQBinaryStore> IndexedBlockDBStore<S> {
  pub fn prev_block_number() -> u64 {
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
      indexed_block: IndexedBlockFull::new_from_block(block_number, block),
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
    if T::TABLE_TYPE == TABLE_TYPE_FUZZY_BLOCK_INDEX {
      KVQTableWrapper::<T, S>::set_ref_at_block(self.store.borrow_mut(), self.block_number, key, value)?;
      let key_bytes = get_real_key_at_block::<T>(&key, self.block_number)?;

      if !self.indexed_block.added_fuzzy_block_keys.contains(&key_bytes){
        self.indexed_block.added_fuzzy_block_keys.append(key_bytes);
      }
    }else if T::TABLE_TYPE == TABLE_TYPE_WRITE_ONCE {
      let old = KVQTableWrapper::<T, S>::get_exact_if_exists_at_block(&self.store, self.block_number, key)?;
      if !old.is_some(){
        KVQTableWrapper::<T, S>::set_ref_at_block(self.store.borrow_mut(), self.block_number, key, value)?;
        let key_bytes = get_real_key_at_block::<T>(&key, self.block_number)?;
        self.indexed_block.added_write_once_keys.append(key_bytes);
      }else if old.unwrap().eq(value){
        return Ok(());
      }else{
        return Err(anyhow::anyhow!("Cannot overwrite value in write-once table"));
      }
    }else if T::TABLE_TYPE == TABLE_TYPE_STANDARD {
      if self.modified_standard_keys_list.contains(key){
        return Ok(());
      }
      let old = KVQTableWrapper::<T, S>::get_leq_kv_at_block(&self.store, self.block_number>0, key, 0)?;

      
      if old.is_some() {
        return Ok(());
      }
      KVQTableWrapper::<T, S>::set_ref_at_block(self.store.borrow_mut(), self.block_number, key, value)?;
      if old.is_some() {
        let v = old.unwrap();
        self.indexed_block.modified_standard_keys.
        if v.eq(value) {
          return Ok(());
        }else{
          if T::TABLE_TYPE == TABLE_TYPE_WRITE_ONCE {
            return Err(anyhow::anyhow!("Cannot overwrite value in write-once table"));
          }else{
            self.indexed_block.
          }
        }
      }
      KVQTableWrapper::<T, S>::set_ref_at_block(self.store.borrow_mut(), self.block_number, key, value)
    }
    if T::TABLE_TYPE != TABLE_TYPE_FUZZY_BLOCK_INDEX {
      let old = KVQTableWrapper::<T, S>::get_exact_if_exists_at_block(&self.store, self.block_number, key)?;
      if old.is_some() {
        let v = old.unwrap();
        if v.eq(value) {
          return Ok(());
        }else{
          if T::TABLE_TYPE == TABLE_TYPE_WRITE_ONCE {
            return Err(anyhow::anyhow!("Cannot overwrite value in write-once table"));
          }else{
            self.indexed_block.
          }
        }
    }
    KVQTableWrapper::<T, S>::set_ref_at_block(self.store.borrow_mut(), self.block_number, key, value)
  }
  pub fn put_many<T: KVQTable>(&mut self, items: &[KVQPair<T::Key, T::Value>]) -> anyhow::Result<()> {
    KVQTableWrapper::<T, S>::set_many_ref_at_block(self.store.borrow_mut(), self.block_number, items)
  }
}*/