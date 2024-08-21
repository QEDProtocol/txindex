use hyper::Response;
use rest::full;
use traits::BoxBody;
use txindex_common::api::response::TxIndexAPIResponse;

pub mod rest;
pub mod traits;
pub mod core;
pub mod chain;


pub trait TxIndexAPIResponseHelper {
    fn into_response(self) -> Response<BoxBody>;
}
impl TxIndexAPIResponseHelper for TxIndexAPIResponse {
    fn into_response(self) -> Response<BoxBody> {
        
        Response::builder()
        .status(self.status)
        .header("Content-Type", self.content_type)
        .body(BoxBody::new(full(self.body)))
        .unwrap()
       
    }
}