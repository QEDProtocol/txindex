# txindex
A modular, fork-tolerant framework for writing composable transaction/ordinal indexers on Bitcoin and Dogecoin.

## WARNING
***This software is still in development, please DO NOT use in production yet.***

## Motivation
Previous ordinal/transaction indexers have been plagued issues when chain forks occur.
txindex seeks to solve this by abstracting the data flow so it is guaranteed to be fork-safe, and to allow roll backs to any block.

**With txindex, developers only have to worry about implementing their indexer's core logic!**

In addition, we want to be able to consume indexers/APIs as modular rust packages so one server can support multiple indexers at once, so txindex allows you to compose/add new indexers whenever you like.

## Usage
#### 1. Implement the database tables you need:
```rust 
use kvq::traits::KVQSerializable;
use serde::{Deserialize, Serialize};
use txindex_common::db::table::core::{KVQTable, TABLE_TYPE_FUZZY_BLOCK_INDEX};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, PartialOrd)]
pub struct SimpleTxCounterDB {
    pub spend_count: u64,
}
impl KVQSerializable for SimpleTxCounterDB {
    fn to_bytes(&self) -> anyhow::Result<Vec<u8>> {
        bincode::serialize(&self).map_err(|err| anyhow::anyhow!("Error serializing SimpleTxCounterDB: {:?}", err))
    }

    fn from_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
        bincode::deserialize(bytes).map_err(|err| anyhow::anyhow!("Error deserializing SimpleTxCounterDB: {:?}", err))
    }
}

impl KVQTable for SimpleTxCounterDB {
    type Key = [u8; 32];
    type Value = Self;
    
    const TABLE_NAME: &'static str = "simple_tx_counter";
    
    const TABLE_ID: u32 = 0x100;
    
    const TABLE_TYPE: u8 = TABLE_TYPE_FUZZY_BLOCK_INDEX;
}
```

#### 2. Implement one or more indexer/worker(s)
```rust
use std::{marker::PhantomData, sync::Arc};

use bitcoin::{Block, Transaction};
use kvq::{cache::KVQBinaryStoreCached, traits::KVQBinaryStoreImmutable};
use txindex_common::{
    db::{chain::TxIndexChainAPI, indexed_block_db::IndexedBlockDBStore},
    worker::traits::TxIndexWorker,
};
use txindex_server::daemon::schema::compute_script_hash;

use crate::tables::tx_counter::SimpleTxCounterDB;

pub struct TxCounterWorker<KVQ: KVQBinaryStoreImmutable, T: TxIndexChainAPI> {
    pub _kvq: PhantomData<KVQ>,
    pub _chain: PhantomData<T>,
}
impl<KVQ: KVQBinaryStoreImmutable, T: TxIndexChainAPI> TxCounterWorker<KVQ, T> {
    fn process_tx(
        db: &mut IndexedBlockDBStore<KVQBinaryStoreCached<KVQ>>,
        _q: Arc<T>,
        _block_number: u64,
        _block: &Block,
        tx: &Transaction,
    ) -> anyhow::Result<()> {
        for input in tx.output.iter() {
            let hash = compute_script_hash(&input.script_pubkey);
            let ctr = db
                .get::<SimpleTxCounterDB>(&hash)?
                .or(Some(SimpleTxCounterDB { spend_count: 0 }))
                .unwrap();
            db.put::<SimpleTxCounterDB>(
                &hash,
                &SimpleTxCounterDB {
                    spend_count: ctr.spend_count + 1,
                },
            )?;
        }
        Ok(())
    }
}
impl<KVQ: KVQBinaryStoreImmutable, T: TxIndexChainAPI> TxIndexWorker<KVQ, T>
    for TxCounterWorker<KVQ, T>
{
    fn process_block(
        db: &mut IndexedBlockDBStore<KVQBinaryStoreCached<KVQ>>,
        q: Arc<T>,
        block_number: u64,
        block: &Block,
    ) -> anyhow::Result<()> {
        for tx in block.txdata.iter() {
            Self::process_tx(db, q.clone(), block_number, block, tx)?;
        }

        Ok(())
    }
}
```

#### 3. Implement any REST APIs you want to expose (with prefix /indexer/)
```rust
use std::sync::Arc;
use txindex_common::{
    api::{response::TxIndexAPIResponse, traits::TxIndexAPIHandler},
    chain::Network,
    db::{
        chain::TxIndexChainAPI, indexed_block_db::IndexedBlockDBStoreReader, kvstore::BaseKVQStore,
    },
};
use txindex_server::api::chain::to_scripthash;
use crate::tables::tx_counter::SimpleTxCounterDB;

pub struct TxCounterAPI<T: TxIndexChainAPI> {
    _chain: std::marker::PhantomData<T>,
}
impl<T: TxIndexChainAPI> TxCounterAPI<T> {
    fn handle_get_request_json(
        network: Network,
        uri: String,
        _chain: Arc<T>,
        indexer_db: IndexedBlockDBStoreReader<BaseKVQStore>,
    ) -> anyhow::Result<Vec<u8>> {
        let address_str = uri.split('/').last().unwrap();
        let sh = to_scripthash("address", address_str, network)
            .map_err(|_| anyhow::anyhow!("invalid address"))?;

        let db = indexer_db
            .get::<SimpleTxCounterDB>(&sh)?
            .or(Some(SimpleTxCounterDB { spend_count: 0 }))
            .unwrap();
        Ok(serde_json::to_vec(&db)?)
    }
}
impl<T: TxIndexChainAPI> TxIndexAPIHandler<T> for TxCounterAPI<T> {
    const PATH_SLUG: &'static str = "/indexer/tx_counter/";

    fn handle_get_request(
        network: Network,
        uri: String,
        chain: std::sync::Arc<T>,
        indexer_db: IndexedBlockDBStoreReader<BaseKVQStore>,
    ) -> TxIndexAPIResponse {
        Self::json_response(Self::handle_get_request_json(
            network, uri, chain, indexer_db,
        ))
    }
}
```


### 4. Setup your root indexer and API handler
Root Indexer/Worker:
```rust
use std::sync::Arc;

use bitcoin::Block;
use kvq::cache::KVQBinaryStoreCached;
use tx_counter::TxCounterWorker;
use txindex_common::{
    db::{indexed_block_db::IndexedBlockDBStore, kvstore::BaseKVQStore},
    worker::traits::TxIndexWorker,
};
use txindex_server::daemon::schema::ChainQuery;

pub mod tx_counter;

pub struct ExampleRootWorker {}

impl TxIndexWorker<BaseKVQStore, ChainQuery> for ExampleRootWorker {
    fn process_block(
        db: &mut IndexedBlockDBStore<KVQBinaryStoreCached<BaseKVQStore>>,
        q: Arc<ChainQuery>,
        block_number: u64,
        block: &Block,
    ) -> anyhow::Result<()> {
        TxCounterWorker::<BaseKVQStore, ChainQuery>::process_block(db, q, block_number, block)?;
        Ok(())
    }
}
```

Root API handler
```rust
use std::sync::Arc;

use hyper::{Method, Response};
use tx_counter::TxCounterAPI;
use txindex_common::{api::traits::TxIndexAPIHandler, config::Config, db::indexed_block_db::IndexedBlockDBStoreReader};
use txindex_server::{api::{core::HttpError, traits::{BoxBody, TxIndexRESTHandler}, TxIndexAPIResponseHelper}, daemon::{query::Query, schema::ChainQuery}};

pub mod tx_counter;
#[derive(Clone, Debug, Copy)]
pub struct ExampleRESTHandler {

}

impl TxIndexRESTHandler for ExampleRESTHandler {
    fn handle_request(
      _method: Method,
      uri: hyper::Uri,
      _body: hyper::body::Bytes,
      q: Arc<Query>,
      config: Arc<Config>,
  ) -> Result<Response<BoxBody>, HttpError> {

    if uri.path().starts_with(TxCounterAPI::<ChainQuery>::PATH_SLUG){
        Ok(TxCounterAPI::<ChainQuery>::handle_get_request(config.network_type, uri.to_string(), q.get_chain_query(), IndexedBlockDBStoreReader{
            store: q.get_kvq_db().clone(),
        }).into_response())
    }else{
        Err(HttpError::not_found("not found".to_string()))
    }
    
  }
    
}
```

### 5. Call start_txindex_server 🎉

```rust
fn main() {
    start_txindex_server::<ExampleRESTHandler, ExampleRootWorker>();
}
```

### License
Copyright 2024 QED, MIT