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