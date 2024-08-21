use std::{collections::HashSet, sync::{Arc, RwLock}};

use bitcoin::BlockHash;
use kvq_store_rocksdb::{compat::RocksDBKVQCDB, KVQRocksDBStore};

use crate::utils::block::HeaderList;
pub type BaseKVQStore = KVQRocksDBStore;
pub type BaseCDBStore = RocksDBKVQCDB;
pub struct TxIndexStore {
    pub txstore_db: BaseCDBStore,
    pub history_db: BaseCDBStore,
    pub cache_db: BaseCDBStore,
    pub indexer_db: Arc<BaseKVQStore>,
    pub added_blockhashes: RwLock<HashSet<BlockHash>>,
    pub indexed_blockhashes: RwLock<HashSet<BlockHash>>,
    pub indexed_headers: RwLock<HeaderList>,
}

impl TxIndexStore {
    pub fn txstore_db(&self) -> &BaseCDBStore {
        &self.txstore_db
    }

    pub fn history_db(&self) -> &BaseCDBStore {
        &self.history_db
    }

    pub fn cache_db(&self) -> &BaseCDBStore {
        &self.cache_db
    }

    pub fn done_initial_sync(&self) -> bool {
        self.txstore_db.get(b"t").is_some()
    }
}
