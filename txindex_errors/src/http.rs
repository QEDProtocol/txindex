use serde::{Deserialize, Serialize};


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpErrorOutput {
  pub status: u32,
  pub is_error: bool,
  pub message: String,
}

impl HttpErrorOutput {
  pub fn new(status: u32, message: String) -> Self {
      HttpErrorOutput {
          status,
          is_error: true,
          message,
      }
  }
}