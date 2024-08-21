use std::num::ParseIntError;

use hyper::StatusCode;

use bitcoin::{hashes::FromSliceError as HashError, hex::HexToArrayError};
use bitcoin::consensus::encode;

use log::warn;
use txindex_errors::http::HttpErrorOutput;


pub const TTL_LONG: u32 = 157_784_630; // ttl for static resources (5 years)
pub const TTL_SHORT: u32 = 10; // ttl for volatie resources

impl From<HttpError> for HttpErrorOutput {
  fn from(e: HttpError) -> Self {
      HttpErrorOutput::new(e.0.as_u16() as u32, e.1)
  }
}
impl From<&HttpError> for HttpErrorOutput {
  fn from(e: &HttpError) -> Self {
      HttpErrorOutput::new(e.0.as_u16() as u32, e.1.to_string())
  }
}

#[derive(Debug, Clone)]
pub struct HttpError(pub StatusCode, pub String);


impl HttpError {
  pub fn not_found(msg: String) -> Self {
      HttpError(StatusCode::NOT_FOUND, msg)
  }
  pub fn to_json_object(&self) -> HttpErrorOutput {
      HttpErrorOutput::from(self)
  }
  pub fn to_json_bytes(&self) -> anyhow::Result<Vec<u8>> {
      let p = serde_json::to_vec(&self.to_json_object());
      match p {
          Ok(v) => anyhow::Ok(v),
          Err(e) => Err(e.into()),
      }

  }
}

impl From<String> for HttpError {
  fn from(msg: String) -> Self {
      HttpError(StatusCode::BAD_REQUEST, msg)
  }
}
impl From<ParseIntError> for HttpError {
  fn from(_e: ParseIntError) -> Self {
      //HttpError::from(e.description().to_string())
      HttpError::from("Invalid number".to_string())
  }
}
impl From<HashError> for HttpError {
  fn from(_e: HashError) -> Self {
      //HttpError::from(e.description().to_string())
      HttpError::from("Invalid hash string".to_string())
  }
}
impl From<hex::FromHexError> for HttpError {
  fn from(_e: hex::FromHexError) -> Self {
      //HttpError::from(e.description().to_string())
      HttpError::from("Invalid hex string".to_string())
  }
}
impl From<bitcoin::address::Error> for HttpError {
  fn from(_e: bitcoin::address::Error) -> Self {
      //HttpError::from(e.description().to_string())
      HttpError::from("Invalid Bitcoin address".to_string())
  }
}
impl From<txindex_errors::core::Error> for HttpError {
  fn from(e: txindex_errors::core::Error) -> Self {
      warn!("errors::Error: {:?}", e);
      match e.description().to_string().as_ref() {
          "getblock RPC error: {\"code\":-5,\"message\":\"Block not found\"}" => {
              HttpError::not_found("Block not found".to_string())
          }
          _ => HttpError::from(e.to_string()),
      }
  }
}
impl From<serde_json::Error> for HttpError {
  fn from(e: serde_json::Error) -> Self {
      HttpError::from(e.to_string())
  }
}
impl From<anyhow::Error> for HttpError {
  fn from(e: anyhow::Error) -> Self {
      HttpError::from(e.to_string())
  }
}
impl From<encode::Error> for HttpError {
  fn from(e: encode::Error) -> Self {
      HttpError::from(e.to_string())
  }
}
impl From<std::string::FromUtf8Error> for HttpError {
  fn from(e: std::string::FromUtf8Error) -> Self {
      HttpError::from(e.to_string())
  }
}

impl From<bitcoin::address::ParseError> for HttpError {
  fn from(e: bitcoin::address::ParseError) -> Self {
      HttpError::from(e.to_string())
  }
}


impl From<HexToArrayError> for HttpError {
  fn from(e: HexToArrayError) -> Self {
      HttpError::from(e.to_string())
  }
}
