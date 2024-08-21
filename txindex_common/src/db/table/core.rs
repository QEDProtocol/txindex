
use std::marker::PhantomData;

use kvq::traits::{KVQBinaryStore, KVQBinaryStoreReader, KVQSerializable};

use super::traits::{KVQTableReaderAtBlock, KVQTableWriterAtBlock};

pub const TABLE_TYPE_FUZZY_BLOCK_INDEX: u8 = 0;
pub const TABLE_TYPE_WRITE_ONCE: u8 = 1;
pub const TABLE_TYPE_STANDARD: u8 = 2;

/// Generic configuration trait.
pub trait KVQTable:
Clone + Sync + Sized + Send + PartialEq
{
  const TABLE_NAME: &'static str;
  const TABLE_ID: u32;
  const TABLE_TYPE: u8;

  type Key: KVQSerializable;
  type Value: KVQSerializable;
}




#[derive(Clone, PartialEq, PartialOrd, Eq, Ord, Debug)]
pub struct KVQTableWrapper<T: KVQTable, S: KVQBinaryStoreReader> {
  _phantom: PhantomData<T>,
  _phantom2: PhantomData<S>,
}

impl<S: KVQBinaryStoreReader, T: KVQTable> KVQTableReaderAtBlock<S, T> for KVQTableWrapper<T, S> {
}
impl<S: KVQBinaryStore, T: KVQTable> KVQTableWriterAtBlock<S, T> for KVQTableWrapper<T, S> {
}