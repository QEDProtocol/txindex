use bitcoin::Address;
use std::{path, str::FromStr, sync::Arc};
use txindex_common::{
    api::{response::TxIndexAPIResponse, traits::TxIndexAPIHandler},
    chain::Network,
    db::{
        chain::TxIndexChainAPI, indexed_block_db::IndexedBlockDBStoreReader, kvstore::BaseKVQStore,
    },
    utils::url::Url,
};

use crate::{
    tables::SimpleTxCounterDB,
    utils::get_scriptpubkey_hash_for_address,
};

pub struct TxCounterAPI<T: TxIndexChainAPI> {
    _chain: std::marker::PhantomData<T>,
}
impl<T: TxIndexChainAPI> TxCounterAPI<T> {
    fn handle_get_request_json(
        network: Network,
        pathname: String,
        _query_string: String,
        _chain: Arc<T>,
        indexer_db: IndexedBlockDBStoreReader<BaseKVQStore>,
    ) -> anyhow::Result<Vec<u8>> {
        let path: Vec<&str> = pathname.split('/').skip(3).collect();

        match (path.get(0), path.get(1), path.get(2)) {
            (Some(&"address"), address, Some(&"stats")) => {
                let address_str = address.ok_or_else(|| anyhow::anyhow!("missing address"))?;
                let p = Address::from_str(address_str)?.require_network(network.into())?;

                let sh = get_scriptpubkey_hash_for_address(&p);

                let db = indexer_db
                    .get::<SimpleTxCounterDB>(&sh)?
                    .or(Some(SimpleTxCounterDB {
                        spend_count: 0,
                        receive_count: 0,
                    }))
                    .unwrap();
                Ok(serde_json::to_vec(&db)?)
            }
            (Some(&"ping"), None, None) => Ok(b"\"pong\"".to_vec()),
            _ => Err(anyhow::anyhow!("not found")),
        }
    }
}
impl<T: TxIndexChainAPI> TxIndexAPIHandler<T> for TxCounterAPI<T> {
    const PATH_SLUG: &'static str = "/indexer/tx_counter/";

    fn handle_get_request(
        network: Network,
        pathname: String,
        query_string: String,
        chain: std::sync::Arc<T>,
        indexer_db: IndexedBlockDBStoreReader<BaseKVQStore>,
    ) -> TxIndexAPIResponse {
        Self::json_response(Self::handle_get_request_json(
            network, pathname, query_string, chain, indexer_db,
        ))
    }
}
