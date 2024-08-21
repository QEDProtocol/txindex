use bitcoin::{Block, Txid};
use kvq::{cache::{CacheValueType, KVQBinaryStoreCached, KVQBinaryStoreCachedTrait}, traits::{KVQBinaryStoreImmutable, KVQBinaryStoreReader, KVQBinaryStoreWriter, KVQPair, KVQSerializable}};
use serde::{Deserialize, Serialize};

use super::{indexed_block_db::IndexedBlockDBStore, kvstore::BaseKVQStore, table::{core::{KVQTable, TABLE_TYPE_FUZZY_BLOCK_INDEX, TABLE_TYPE_STANDARD, TABLE_TYPE_WRITE_ONCE}, traits::{get_real_key_at_block, get_table_type_for_raw_key}}};

use kvq::traits::KVQBinaryStoreWriterImmutable;

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct IndexedBlockMetadata {
  pub block_number: u64,
  pub block_time: u64,
  pub block_hash: [u8; 32],
}

impl IndexedBlockMetadata {
  pub fn new(block_number: u64, block_time: u64, block_hash: [u8; 32]) -> Self {
    Self {
      block_number,
      block_time,
      block_hash,
    }
  }
  pub fn new_from_block(block_number: u64, block: &Block) -> Self {
    let mut block_hash: [u8; 32] = [0u8; 32];
    block_hash.copy_from_slice(block.block_hash().as_raw_hash().as_ref());
    Self {
      block_number,
      block_time: block.header.time as u64,
      block_hash,
    }
  }
}
impl TryFrom<&Block> for IndexedBlockMetadata {
  type Error = anyhow::Error;
  fn try_from(block: &Block) -> anyhow::Result<Self> {
    let mut block_hash: [u8; 32] = [0u8; 32];
    block_hash.copy_from_slice(block.block_hash().as_raw_hash().as_ref());
    
    Ok(Self {
      block_number: block.bip34_block_height()? as u64,
      block_time: block.header.time as u64,
      block_hash,
    })
  }
}
#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct SerializedIndexedBlockAction {
  pub txid: [u8; 32],
  pub worker_id: u32,
  pub action_type: u32,
  pub action_data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct SerializedRemovedStandardKey {
  pub key: Vec<u8>,
  pub value: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct SerializedModifiedStandardKey {
  pub key: Vec<u8>,
  pub new_value: Vec<u8>,
  pub old_value: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct SerializedAddedStandardKey {
  pub key: Vec<u8>,
  pub new_value: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct IndexedBlockFull {
  pub metadata: IndexedBlockMetadata,
  pub actions: Vec<SerializedIndexedBlockAction>,
  pub added_fuzzy_block_keys: Vec<Vec<u8>>,
  pub added_write_once_keys: Vec<Vec<u8>>,
  pub removed_standard_keys: Vec<SerializedRemovedStandardKey>,
  pub modified_standard_keys: Vec<SerializedModifiedStandardKey>,
  pub added_standard_keys: Vec<SerializedAddedStandardKey>,
}
impl KVQTable for IndexedBlockFull {
  type Key = u64;
  type Value = Self;
  const TABLE_TYPE: u8 = TABLE_TYPE_WRITE_ONCE;
  
  const TABLE_NAME: &'static str = "indexed_block";
  
  const TABLE_ID: u32 = 0;
}

impl IndexedBlockFull {
  pub fn new(metadata: IndexedBlockMetadata) -> Self {
    Self {
      metadata: metadata,
      actions: Vec::new(),
      added_fuzzy_block_keys: Vec::new(),
      added_write_once_keys: Vec::new(),
      removed_standard_keys: Vec::new(),
      modified_standard_keys: Vec::new(),
      added_standard_keys: Vec::new(),
    }
  }
  pub fn new_from_block(block_number: u64, block: &Block) -> Self {
    Self {
      metadata: IndexedBlockMetadata::new_from_block(block_number, block),
      actions: Vec::new(),
      added_fuzzy_block_keys: Vec::new(),
      added_write_once_keys: Vec::new(),
      removed_standard_keys: Vec::new(),
      modified_standard_keys: Vec::new(),
      added_standard_keys: Vec::new(),
    }
  }
  pub fn emit_action(&mut self, action: SerializedIndexedBlockAction) {
    self.actions.push(action);
  }
  pub fn emit_action_from_txid<P: KVQSerializable>(&mut self, txid: &Txid, worker_id: u32, action_type: u32, action_data: &P) {
    let mut txid_bytes = [0u8; 32];
    txid_bytes.copy_from_slice(txid.as_ref());

    self.actions.push(SerializedIndexedBlockAction{
      txid: txid_bytes,
      worker_id,
      action_type,
      action_data: action_data.to_bytes().unwrap(),
    });
  }
  
  pub fn save_from_db_store(db_store: IndexedBlockDBStore<KVQBinaryStoreCached<BaseKVQStore>>) -> anyhow::Result<()> {
    
    let mut indexed_block = IndexedBlockFull::new(db_store.metadata.clone());

    for (key, vt) in db_store.store.map.iter() {
      let key_type = get_table_type_for_raw_key(&key);
      match vt {
        CacheValueType::Bytes(new_value) => {
          match key_type {
            TABLE_TYPE_WRITE_ONCE => {
              indexed_block.added_write_once_keys.push(key.to_vec());
            },
            TABLE_TYPE_FUZZY_BLOCK_INDEX => {
              indexed_block.added_fuzzy_block_keys.push(key.to_vec());
            },
            TABLE_TYPE_STANDARD => {
              let old_value = db_store.store.get_exact_if_exists(key)?;
              if old_value.is_none() {
                indexed_block.added_standard_keys.push(SerializedAddedStandardKey{
                  key: key.to_vec(),
                  new_value: new_value.to_vec(),
                });
              }else{
                indexed_block.modified_standard_keys.push(SerializedModifiedStandardKey{
                  key: key.to_vec(),
                  new_value: new_value.to_vec(),
                  old_value: old_value.unwrap(),
                });
              }
            },
            _ => anyhow::bail!("Unknown key type"),
          }
          
        },
        CacheValueType::Removed => {
          match key_type {
            TABLE_TYPE_STANDARD => {
              let old_value = db_store.store.get_exact_if_exists(key)?;
              if old_value.is_some() {
                indexed_block.removed_standard_keys.push(SerializedRemovedStandardKey{
                  key: key.to_vec(),
                  value: old_value.unwrap(),
                });
              }
            },
            _ => anyhow::bail!("invalid key type for removal"),
          }
        },
      }

    }
    indexed_block.actions.extend_from_slice(&db_store.actions);
    let mut db_store = db_store.store;
    db_store.flush_simple()?;





    db_store.store.imm_set(get_real_key_at_block::<IndexedBlockFull>(&indexed_block.metadata.block_number,indexed_block.metadata.block_number)?, indexed_block.to_bytes()?)?;

    Ok(())

  

    
  }
}

impl KVQSerializable for IndexedBlockMetadata {
  fn to_bytes(&self) -> anyhow::Result<Vec<u8>> {
    Ok(bincode::serialize(self)?)
  }
  fn from_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
    Ok(bincode::deserialize(bytes)?)
  }
}

impl KVQSerializable for IndexedBlockFull {
  fn to_bytes(&self) -> anyhow::Result<Vec<u8>> {
    Ok(bincode::serialize(self)?)
  }
  fn from_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
    Ok(bincode::deserialize(bytes)?)
  }
}

impl KVQSerializable for SerializedIndexedBlockAction {
  fn to_bytes(&self) -> anyhow::Result<Vec<u8>> {
    Ok(bincode::serialize(self)?)
  }
  fn from_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
    Ok(bincode::deserialize(bytes)?)
  }
}
