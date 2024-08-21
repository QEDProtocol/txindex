use txindex_errors::core::Result;
pub trait CookieGetter: Send + Sync {
  fn get(&self) -> Result<Vec<u8>>;
}