#[derive(Debug, Clone)]
pub struct TxIndexAPIResponse {
  pub status: u16,
  pub content_type: String,
  pub body: Vec<u8>,
}
