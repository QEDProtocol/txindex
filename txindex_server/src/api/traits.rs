use std::sync::Arc;

use hyper::{body::Bytes, Method, Response};
use txindex_common::config::Config;

use crate::daemon::query::Query;

use super::core::HttpError;
pub type BoxBody = http_body_util::combinators::BoxBody<Bytes, hyper::Error>;


pub trait TxIndexRESTHandler: Clone {
  fn handle_request(
    method: Method,
    uri: hyper::Uri,
    body: hyper::body::Bytes,
    q: Arc<Query>,
    config: Arc<Config>,
) -> Result<Response<BoxBody>, HttpError>;
}