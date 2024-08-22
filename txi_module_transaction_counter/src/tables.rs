use kvq::traits::KVQSerializable;
use serde::{Deserialize, Serialize};
use txindex_common::db::table::{core::{KVQTable, TABLE_TYPE_FUZZY_BLOCK_INDEX}, traits::get_table_id_hash};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, PartialOrd)]
pub struct SimpleTxCounterDB {
    pub spend_count: u64,
    pub receive_count: u64,
}
impl KVQSerializable for SimpleTxCounterDB {
    fn to_bytes(&self) -> anyhow::Result<Vec<u8>> {
        bincode::serialize(&self).map_err(|err| anyhow::anyhow!("Error serializing SimpleTxCounterDB: {:?}", err))
    }

    fn from_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
        bincode::deserialize(bytes).map_err(|err| anyhow::anyhow!("Error deserializing SimpleTxCounterDB: {:?}", err))
    }
}
impl KVQTable for SimpleTxCounterDB {
    type Key = [u8; 32];
    type Value = Self;
    
    const TABLE_NAME: &'static str = "simple_tx_counter";
    
    const TABLE_ID: u32 = get_table_id_hash(Self::TABLE_NAME);
    
    const TABLE_TYPE: u8 = TABLE_TYPE_FUZZY_BLOCK_INDEX;
}


