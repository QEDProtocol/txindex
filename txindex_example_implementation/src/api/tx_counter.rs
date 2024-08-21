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
