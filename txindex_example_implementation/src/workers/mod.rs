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
