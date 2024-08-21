use kvq::traits::{
    KVQBinaryStore, KVQBinaryStoreReader, KVQPair, KVQSerializable,
};

use super::core::{KVQTable, TABLE_TYPE_FUZZY_BLOCK_INDEX};
const MAGIC_IMPOSSIBLE_BLOCK_NUMBER: u64 = 0xFFFFFFFFFFFFFFFFu64;

pub fn get_table_type_for_raw_key(raw_key: &[u8]) -> u8 {
    raw_key[0] >> 4
}
pub fn get_real_key_at_block<T: KVQTable>(
    key: &T::Key,
    block_number: u64,
) -> anyhow::Result<Vec<u8>> {
    let mut real_key_bytes = (T::TABLE_ID | (((T::TABLE_TYPE&0xf) as u32)<<28u32)).to_be_bytes().to_vec();
    real_key_bytes.extend_from_slice(&key.to_bytes()?);
    if T::TABLE_TYPE == TABLE_TYPE_FUZZY_BLOCK_INDEX {
        real_key_bytes.extend_from_slice(&block_number.to_be_bytes());
    }
    Ok(real_key_bytes)
}
fn resolve_fuzzy_bytes<T: KVQTable>(fuzzy_bytes: usize) -> usize {
    if T::TABLE_TYPE == TABLE_TYPE_FUZZY_BLOCK_INDEX {
        fuzzy_bytes + 8
    } else {
        fuzzy_bytes
    }
}
pub fn deserialize_raw_key_for_table<T: KVQTable>(raw_key: &[u8]) -> anyhow::Result<T::Key> {
    let key_bytes = if T::TABLE_TYPE == TABLE_TYPE_FUZZY_BLOCK_INDEX {
        &raw_key[4..raw_key.len() - 8]
    } else {
        &raw_key[4..]
    };
    T::Key::from_bytes(key_bytes)
}
#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct KVQDecodedRawTableKey {
    pub table_id: u32,
    pub block_number: u64,
    pub table_type: u8,
    pub key: Vec<u8>,
}
impl KVQDecodedRawTableKey {
    pub fn new(table_id: u32, block_number: u64, table_type: u8, key: Vec<u8>) -> Self {
        Self {
            table_id,
            block_number,
            table_type,
            key,
        }
    }
    pub fn new_ref(table_id: u32, block_number: u64, table_type: u8, key: &[u8]) -> Self {
        Self {
            table_id,
            block_number,
            table_type,
            key: key.to_vec(),
        }
    }
    pub fn new_with_table<T: KVQTable>(key: Vec<u8>, block_number: u64) -> Self {
        Self {
            table_id: T::TABLE_ID,
            block_number,
            table_type: T::TABLE_TYPE,
            key,
        }
    }
}

impl KVQSerializable for KVQDecodedRawTableKey {
    fn to_bytes(&self) -> anyhow::Result<Vec<u8>> {
        let mut real_key_bytes = (self.table_id | (((self.table_type&0xf) as u32)<<28u32)).to_be_bytes().to_vec();
        real_key_bytes.extend_from_slice(&self.key.to_bytes()?);
        if self.table_type == TABLE_TYPE_FUZZY_BLOCK_INDEX {
            real_key_bytes.extend_from_slice(&self.block_number.to_be_bytes());
        }
        Ok(real_key_bytes)
    }

    fn from_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
        let table_id = u32::from_be_bytes(bytes[0..4].try_into()?);
        let table_type = (table_id >> 28) as u8;
        let table_id = table_id & 0x0FFFFFFF;
        if table_type == TABLE_TYPE_FUZZY_BLOCK_INDEX {
            let key = bytes[4..(bytes.len() - 8)].to_vec();
            let block_number = u64::from_be_bytes(bytes[bytes.len() - 8..].try_into()?);
            Ok(Self {
                table_id,
                block_number,
                table_type,
                key,
            })
        } else {
            let key = bytes[4..].to_vec();
            Ok(Self {
                table_id,
                block_number: MAGIC_IMPOSSIBLE_BLOCK_NUMBER,
                table_type,
                key,
            })
        }
    }
}
#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct KVQTableKeyWithBlockNumber<T: KVQTable> {
    pub key: T::Key,
    pub block_number: u64,
}

impl<T: KVQTable> From<KVQTableKeyWithBlockNumber<T>> for KVQDecodedRawTableKey {
    fn from(k: KVQTableKeyWithBlockNumber<T>) -> Self {
        KVQDecodedRawTableKey::new_with_table::<T>(k.key.to_bytes().unwrap(), k.block_number)
    }
}
impl<T: KVQTable> KVQTableKeyWithBlockNumber<T> {
    pub fn new(key: T::Key, block_number: u64) -> Self {
        if T::TABLE_TYPE != TABLE_TYPE_FUZZY_BLOCK_INDEX {
            Self {
                key,
                block_number: MAGIC_IMPOSSIBLE_BLOCK_NUMBER,
            }
        } else {
            Self { key, block_number }
        }
    }
    pub fn new_basic(key: T::Key) -> Self {
        if T::TABLE_TYPE == TABLE_TYPE_FUZZY_BLOCK_INDEX {
            panic!("This table requires a block number");
        }
        Self {
            key,
            block_number: MAGIC_IMPOSSIBLE_BLOCK_NUMBER,
        }
    }
    pub fn get_bytes(&self) -> anyhow::Result<Vec<u8>> {
        get_real_key_at_block::<T>(&self.key, self.block_number)
    }
}
impl<T: KVQTable> KVQSerializable for KVQTableKeyWithBlockNumber<T> {
    fn to_bytes(&self) -> anyhow::Result<Vec<u8>> {
        get_real_key_at_block::<T>(&self.key, self.block_number)
    }
    fn from_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
        if T::TABLE_TYPE == TABLE_TYPE_FUZZY_BLOCK_INDEX {
            let value = T::Key::from_bytes(&bytes[4..bytes.len() - 8])?;
            let block_number = u64::from_be_bytes(bytes[bytes.len() - 8..].try_into()?);
            Ok(Self {
                key: value,
                block_number,
            })
        } else {
            let value = T::Key::from_bytes(&bytes[4..])?;
            Ok(Self {
                key: value,
                block_number: MAGIC_IMPOSSIBLE_BLOCK_NUMBER,
            })
        }
    }
}

pub trait KVQTableReaderAtBlock<S: KVQBinaryStoreReader, T: KVQTable> {
    fn get_exact_if_exists_at_block(
        s: &S,
        block_number: u64,
        key: &T::Key,
    ) -> anyhow::Result<Option<T::Value>> {
        let r = s.get_exact_if_exists(&get_real_key_at_block::<T>(key, block_number)?)?;
        if r.is_some() {
            let result = T::Value::from_bytes(&r.unwrap())?;
            Ok(Some(result))
        } else {
            Ok(None)
        }
    }
    fn get_exact_at_block(s: &S, block_number: u64, key: &T::Key) -> anyhow::Result<T::Value> {
        let r = s.get_exact(&get_real_key_at_block::<T>(key, block_number)?)?;
        Ok(T::Value::from_bytes(&r)?)
    }

    fn get_leq_kv_at_block(
        s: &S,
        block_number: u64,
        key: &T::Key,
        fuzzy_bytes: usize,
    ) -> anyhow::Result<Option<KVQPair<T::Key, T::Value>>> {
        let r = s.get_leq_kv(
            &get_real_key_at_block::<T>(key, block_number)?,
            resolve_fuzzy_bytes::<T>(fuzzy_bytes),
        )?;
        match r {
            Some(kv) => Ok(Some(KVQPair {
                key: deserialize_raw_key_for_table::<T>(&kv.key)?,
                value: T::Value::from_bytes(&kv.value)?,
            })),
            None => Ok(None),
        }
    }

    fn get_many_exact_at_block(
        s: &S,
        block_number: u64,
        keys: &[T::Key],
    ) -> anyhow::Result<Vec<T::Value>> {
        let keys_bytes = keys
            .iter()
            .map(|k| get_real_key_at_block::<T>(k, block_number))
            .collect::<anyhow::Result<Vec<Vec<u8>>>>()?;
        let values_bytes = s.get_many_exact(&keys_bytes)?;
        let values = values_bytes
            .iter()
            .map(|r| T::Value::from_bytes(r))
            .collect::<anyhow::Result<Vec<T::Value>>>();
        Ok(values?)
    }

    fn get_leq_at_block(
        s: &S,
        block_number: u64,
        key: &T::Key,
        fuzzy_bytes: usize,
    ) -> anyhow::Result<Option<T::Value>> {
        let r = s.get_leq(
            &get_real_key_at_block::<T>(key, block_number)?,
            resolve_fuzzy_bytes::<T>(fuzzy_bytes),
        )?;
        match r {
            Some(v) => Ok(Some(T::Value::from_bytes(&v)?)),
            None => Ok(None),
        }
    }

    fn get_many_leq_at_block(
        s: &S,
        block_number: u64,
        keys: &[T::Key],
        fuzzy_bytes: usize,
    ) -> anyhow::Result<Vec<Option<T::Value>>> {
        let keys_bytes = keys
            .iter()
            .map(|k| get_real_key_at_block::<T>(k, block_number))
            .collect::<anyhow::Result<Vec<Vec<u8>>>>()?;
        let values_bytes = s.get_many_leq(&keys_bytes, resolve_fuzzy_bytes::<T>(fuzzy_bytes))?;
        let values = values_bytes
            .iter()
            .map(|r| {
                Ok(match r {
                    Some(v) => Some(T::Value::from_bytes(v)?),
                    None => None,
                })
            })
            .collect::<anyhow::Result<Vec<Option<T::Value>>>>();
        Ok(values?)
    }

    fn get_many_leq_kv_at_block(
        s: &S,
        block_number: u64,
        keys: &[T::Key],
        fuzzy_bytes: usize,
    ) -> anyhow::Result<Vec<Option<KVQPair<KVQTableKeyWithBlockNumber<T>, T::Value>>>> {
        let keys_bytes = keys
            .iter()
            .map(|k| get_real_key_at_block::<T>(k, block_number))
            .collect::<anyhow::Result<Vec<Vec<u8>>>>()?;
        let kvs_bytes = s.get_many_leq_kv(&keys_bytes, resolve_fuzzy_bytes::<T>(fuzzy_bytes))?;
        let kvs: anyhow::Result<Vec<Option<KVQPair<KVQTableKeyWithBlockNumber<T>, T::Value>>>> =
            kvs_bytes
                .iter()
                .map(|r| {
                    Ok(match r {
                        Some(kv) => Some(KVQPair {
                            key: KVQTableKeyWithBlockNumber::<T>::from_bytes(&kv.key)?,
                            value: T::Value::from_bytes(&kv.value)?,
                        }),
                        None => None,
                    })
                })
                .collect();
        Ok(kvs?)
    }

    fn get_fuzzy_range_leq_kv_at_block(
        s: &S,
        block_number: u64,
        key: &T::Key,
        fuzzy_bytes: usize,
    ) -> anyhow::Result<Vec<KVQPair<KVQTableKeyWithBlockNumber<T>, T::Value>>> {
        let key = get_real_key_at_block::<T>(key, block_number)?;
        s.get_fuzzy_range_leq_kv(&key, resolve_fuzzy_bytes::<T>(fuzzy_bytes))?
            .into_iter()
            .map(|kv| {
                Ok(KVQPair {
                    key: KVQTableKeyWithBlockNumber::<T>::from_bytes(&kv.key)?,
                    value: T::Value::from_bytes(&kv.value)?,
                })
            })
            .collect()
    }

    fn get_exact_if_exists_combo_at_block(
        s: &S,
        key: &KVQTableKeyWithBlockNumber<T>,
    ) -> anyhow::Result<Option<T::Value>> {
        let r = s.get_exact_if_exists(&key.to_bytes()?)?;
        if r.is_some() {
            let result = T::Value::from_bytes(&r.unwrap())?;
            Ok(Some(result))
        } else {
            Ok(None)
        }
    }
    fn get_exact_combo_at_block(s: &S, key: &KVQTableKeyWithBlockNumber<T>) -> anyhow::Result<T::Value> {
        let r = s.get_exact(&key.to_bytes()?)?;
        Ok(T::Value::from_bytes(&r)?)
    }

    fn get_leq_kv_combo_at_block(
        s: &S,
        key: &KVQTableKeyWithBlockNumber<T>,
        fuzzy_bytes: usize,
    ) -> anyhow::Result<Option<KVQPair<KVQTableKeyWithBlockNumber<T>, T::Value>>> {
        let r = s.get_leq_kv(
            &key.to_bytes()?,
            resolve_fuzzy_bytes::<T>(fuzzy_bytes),
        )?;
        match r {
            Some(kv) => Ok(Some(KVQPair {
                key: KVQTableKeyWithBlockNumber::<T>::from_bytes(&kv.key)?,
                value: T::Value::from_bytes(&kv.value)?,
            })),
            None => Ok(None),
        }
    }

    fn get_many_exact_combo_at_block(
        s: &S,
        keys: &[KVQTableKeyWithBlockNumber<T>],
    ) -> anyhow::Result<Vec<T::Value>> {
        let keys_bytes = keys
            .iter()
            .map(|k| k.to_bytes())
            .collect::<anyhow::Result<Vec<Vec<u8>>>>()?;
        let values_bytes = s.get_many_exact(&keys_bytes)?;
        let values = values_bytes
            .iter()
            .map(|r| T::Value::from_bytes(r))
            .collect::<anyhow::Result<Vec<T::Value>>>();
        Ok(values?)
    }

    fn get_leq_combo_at_block(
        s: &S,
        key: &KVQTableKeyWithBlockNumber<T>,
        fuzzy_bytes: usize,
    ) -> anyhow::Result<Option<T::Value>> {
        let r = s.get_leq(
            &key.to_bytes()?,
            resolve_fuzzy_bytes::<T>(fuzzy_bytes),
        )?;
        match r {
            Some(v) => Ok(Some(T::Value::from_bytes(&v)?)),
            None => Ok(None),
        }
    }

    fn get_many_leq_combo_at_block(
        s: &S,
        keys: &[KVQTableKeyWithBlockNumber<T>],
        fuzzy_bytes: usize,
    ) -> anyhow::Result<Vec<Option<T::Value>>> {
        let keys_bytes = keys
            .iter()
            .map(|k| k.to_bytes())
            .collect::<anyhow::Result<Vec<Vec<u8>>>>()?;
        let values_bytes = s.get_many_leq(&keys_bytes, resolve_fuzzy_bytes::<T>(fuzzy_bytes))?;
        let values = values_bytes
            .iter()
            .map(|r| {
                Ok(match r {
                    Some(v) => Some(T::Value::from_bytes(v)?),
                    None => None,
                })
            })
            .collect::<anyhow::Result<Vec<Option<T::Value>>>>();
        Ok(values?)
    }

    fn get_many_leq_kv_combo_at_block(
        s: &S,
        keys: &[KVQTableKeyWithBlockNumber<T>],
        fuzzy_bytes: usize,
    ) -> anyhow::Result<Vec<Option<KVQPair<KVQTableKeyWithBlockNumber<T>, T::Value>>>> {
        let keys_bytes = keys
            .iter()
            .map(|k| k.to_bytes())
            .collect::<anyhow::Result<Vec<Vec<u8>>>>()?;
        let kvs_bytes = s.get_many_leq_kv(&keys_bytes, resolve_fuzzy_bytes::<T>(fuzzy_bytes))?;
        let kvs: anyhow::Result<Vec<Option<KVQPair<KVQTableKeyWithBlockNumber<T>, T::Value>>>> =
            kvs_bytes
                .iter()
                .map(|r| {
                    Ok(match r {
                        Some(kv) => Some(KVQPair {
                            key: KVQTableKeyWithBlockNumber::<T>::from_bytes(&kv.key)?,
                            value: T::Value::from_bytes(&kv.value)?,
                        }),
                        None => None,
                    })
                })
                .collect();
        Ok(kvs?)
    }

    fn get_fuzzy_range_leq_kv_combo_at_block(
        s: &S,
        key: &KVQTableKeyWithBlockNumber<T>,
        fuzzy_bytes: usize,
    ) -> anyhow::Result<Vec<KVQPair<KVQTableKeyWithBlockNumber<T>, T::Value>>> {
        let key = key.to_bytes()?;
        s.get_fuzzy_range_leq_kv(&key, resolve_fuzzy_bytes::<T>(fuzzy_bytes))?
            .into_iter()
            .map(|kv| {
                Ok(KVQPair {
                    key: KVQTableKeyWithBlockNumber::<T>::from_bytes(&kv.key)?,
                    value: T::Value::from_bytes(&kv.value)?,
                })
            })
            .collect()
    }
}

pub trait KVQTableWriterAtBlock<S: KVQBinaryStore, T: KVQTable>:
    KVQTableReaderAtBlock<S, T>
{
    fn set_ref_at_block(
        s: &mut S,
        block_number: u64,
        key: &T::Key,
        value: &T::Value,
    ) -> anyhow::Result<()> {
        s.set(
            get_real_key_at_block::<T>(key, block_number)?,
            value.to_bytes()?,
        )
    }
    fn set_at_block(
        s: &mut S,
        block_number: u64,
        key: T::Key,
        value: T::Value,
    ) -> anyhow::Result<()> {
        s.set(
            get_real_key_at_block::<T>(&key, block_number)?,
            value.to_bytes()?,
        )
    }

    fn set_many_ref_at_block<'a>(
        s: &mut S,
        block_number: u64,
        items: &[KVQPair<&'a T::Key, &'a T::Value>],
    ) -> anyhow::Result<()> {
        let pairs: anyhow::Result<Vec<KVQPair<Vec<u8>, Vec<u8>>>> = items
            .iter()
            .map(|kv| {
                Ok(KVQPair {
                    key: get_real_key_at_block::<T>(kv.key, block_number)?,
                    value: kv.value.to_bytes()?,
                })
            })
            .collect();
        s.set_many_vec(pairs?)
    }

    fn set_many_at_block(
        s: &mut S,
        block_number: u64,
        items: &[KVQPair<T::Key, T::Value>],
    ) -> anyhow::Result<()> {
        let pairs: anyhow::Result<Vec<KVQPair<Vec<u8>, Vec<u8>>>> = items
            .iter()
            .map(|kv| {
                Ok(KVQPair {
                    key: get_real_key_at_block::<T>(&kv.key, block_number)?,
                    value: kv.value.to_bytes()?,
                })
            })
            .collect();
        s.set_many_vec(pairs?)
    }

    fn delete_at_block(s: &mut S, block_number: u64, key: &T::Key) -> anyhow::Result<bool> {
        s.delete(&get_real_key_at_block::<T>(key, block_number)?)
    }

    fn delete_many_at_block(
        s: &mut S,
        block_number: u64,
        keys: &[T::Key],
    ) -> anyhow::Result<Vec<bool>> {
        let mut results: Vec<bool> = Vec::with_capacity(keys.len());

        for k in keys {
            let r = s.delete(&get_real_key_at_block::<T>(k, block_number)?)?;
            results.push(r)
        }
        Ok(results)
    }

    fn set_many_split_ref_at_block(
        s: &mut S,
        block_number: u64,
        keys: &[T::Key],
        values: &[T::Value],
    ) -> anyhow::Result<()> {
        if keys.len() != values.len() {
            return Err(anyhow::anyhow!("Keys and values must have the same length"));
        }
        let mut keys_bytes: Vec<Vec<u8>> = Vec::with_capacity(keys.len());
        let mut values_bytes: Vec<Vec<u8>> = Vec::with_capacity(values.len());
        for (k, v) in keys.iter().zip(values.iter()) {
            keys_bytes.push(get_real_key_at_block::<T>(k, block_number)?);
            values_bytes.push(v.to_bytes()?);
        }

        s.set_many_split_ref(&keys_bytes, &values_bytes)
    }

    // start combos

    fn set_ref_combo_at_block(
        s: &mut S,
        key: &KVQTableKeyWithBlockNumber<T>,
        value: &T::Value,
    ) -> anyhow::Result<()> {
        s.set(
            key.to_bytes()?,
            value.to_bytes()?,
        )
    }
    fn set_combo_at_block(
        s: &mut S,
        key: KVQTableKeyWithBlockNumber<T>,
        value: T::Value,
    ) -> anyhow::Result<()> {
        s.set(
            key.to_bytes()?,
            value.to_bytes()?,
        )
    }

    fn set_many_ref_combo_at_block<'a>(
        s: &mut S,
        items: &[KVQPair<&'a KVQTableKeyWithBlockNumber<T>, &'a T::Value>],
    ) -> anyhow::Result<()> {
        let pairs: anyhow::Result<Vec<KVQPair<Vec<u8>, Vec<u8>>>> = items
            .iter()
            .map(|kv| {
                Ok(KVQPair {
                    key: kv.key.to_bytes()?,
                    value: kv.value.to_bytes()?,
                })
            })
            .collect();
        s.set_many_vec(pairs?)
    }

    fn set_many_combo_at_block(
        s: &mut S,
        items: &[KVQPair<KVQTableKeyWithBlockNumber<T>, T::Value>],
    ) -> anyhow::Result<()> {
        let pairs: anyhow::Result<Vec<KVQPair<Vec<u8>, Vec<u8>>>> = items
            .iter()
            .map(|kv| {
                Ok(KVQPair {
                    key: kv.key.to_bytes()?,
                    value: kv.value.to_bytes()?,
                })
            })
            .collect();
        s.set_many_vec(pairs?)
    }

    fn delete_combo_at_block(s: &mut S, key: &KVQTableKeyWithBlockNumber<T>) -> anyhow::Result<bool> {
        s.delete(&key.to_bytes()?)
    }

    fn delete_many_combo_at_block(
        s: &mut S,
        keys: &[KVQTableKeyWithBlockNumber<T>],
    ) -> anyhow::Result<Vec<bool>> {
        let mut results: Vec<bool> = Vec::with_capacity(keys.len());

        for k in keys {
            let r = s.delete(&k.to_bytes()?)?;
            results.push(r)
        }
        Ok(results)
    }

    fn set_many_split_ref_combo_at_block(
        s: &mut S,
        keys: &[KVQTableKeyWithBlockNumber<T>],
        values: &[T::Value],
    ) -> anyhow::Result<()> {
        if keys.len() != values.len() {
            return Err(anyhow::anyhow!("Keys and values must have the same length"));
        }
        let mut keys_bytes: Vec<Vec<u8>> = Vec::with_capacity(keys.len());
        let mut values_bytes: Vec<Vec<u8>> = Vec::with_capacity(values.len());
        for (k, v) in keys.iter().zip(values.iter()) {
            keys_bytes.push(k.to_bytes()?);
            values_bytes.push(v.to_bytes()?);
        }

        s.set_many_split_ref(&keys_bytes, &values_bytes)
    }
}
