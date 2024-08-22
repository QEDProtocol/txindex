use std::sync::Arc;

use bitcoin::Block;
use hyper::{Method, Response};
use kvq::cache::KVQBinaryStoreCached;
use txi_module_transaction_counter::{api::TxCounterAPI, worker::TxCounterWorker};
use txindex_common::{
    api::traits::TxIndexAPIHandler,
    config::Config,
    db::{
        indexed_block_db::{IndexedBlockDBStore, IndexedBlockDBStoreReader},
        kvstore::BaseKVQStore,
    },
    worker::traits::TxIndexWorker,
};
use txindex_server::{
    api::{
        core::HttpError,
        traits::{BoxBody, TxIndexRESTHandler},
        TxIndexAPIResponseHelper,
    },
    daemon::{query::Query, schema::ChainQuery},
    server::start_txindex_server,
};

#[derive(Clone, Debug, Copy)]
pub struct ExampleRESTHandler {}
impl TxIndexRESTHandler for ExampleRESTHandler {
    fn handle_request(
        _method: Method,
        uri: hyper::Uri,
        _body: hyper::body::Bytes,
        q: Arc<Query>,
        config: Arc<Config>,
    ) -> Result<Response<BoxBody>, HttpError> {
        if uri
            .path()
            .starts_with(TxCounterAPI::<ChainQuery>::PATH_SLUG)
        {
            Ok(TxCounterAPI::<ChainQuery>::handle_get_request(
                config.network_type,
                uri.path().to_string(),
                uri.query().unwrap_or("").to_string(),
                q.get_chain_query(),
                IndexedBlockDBStoreReader {
                    store: q.get_kvq_db().clone(),
                },
            )
            .into_response())
        } else {
            Err(HttpError::not_found("not found".to_string()))
        }
    }
}
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
fn main() {
    start_txindex_server::<ExampleRESTHandler, ExampleRootWorker>();
}
