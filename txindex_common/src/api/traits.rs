use std::sync::Arc;

use crate::{chain::Network, db::{chain::TxIndexChainAPI, indexed_block_db::IndexedBlockDBStoreReader, kvstore::BaseKVQStore}};

use super::response::TxIndexAPIResponse;

pub trait TxIndexAPIHandler<T: TxIndexChainAPI> {
  const PATH_SLUG: &'static str;
  fn handle_get_request(
    network: Network,
    uri: String,
    chain: Arc<T>,
    indexer_db: IndexedBlockDBStoreReader<BaseKVQStore>,
  ) -> TxIndexAPIResponse;
  fn json_response(
    body: anyhow::Result<Vec<u8>>,
  ) -> TxIndexAPIResponse {
    if body.is_err() {
      TxIndexAPIResponse {
        status: 500,
        content_type: "application/json".to_string(),
        body: format!("{{\"error\": \"{}\"}}", "error processing request").into_bytes(),
      }
    }else{
      TxIndexAPIResponse {
        status: 200,
        content_type: "application/json".to_string(),
        body: body.unwrap(),
      }
    }
  }
}

