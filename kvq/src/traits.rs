use serde::Deserialize;
use serde::Serialize;

pub struct KVQPair<K, V> {
    pub key: K,
    pub value: V,
}

#[derive(Serialize, Deserialize, PartialEq, Clone)]
pub struct KVQPairSerializable<K, V> {
    pub key: K,
    pub value: V,
}
impl<K: Serialize + Clone, V: Serialize + Clone> Serialize for KVQPair<K, V> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let serializable = KVQPairSerializable {
            key: self.key.clone(),
            value: self.value.clone(),
        };
        serializable.serialize(serializer)
    }
}
impl<'de, K: Deserialize<'de>, V: Deserialize<'de>> Deserialize<'de> for KVQPair<K, V> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw = KVQPairSerializable::deserialize(deserializer)?;
        Ok(KVQPair {
            key: raw.key,
            value: raw.value,
        })
    }
}

pub trait KVQSerializable: Clone + PartialEq {
    fn to_bytes(&self) -> anyhow::Result<Vec<u8>>;
    fn from_bytes(bytes: &[u8]) -> anyhow::Result<Self>;
}

pub fn unwrap_kv_vec_result<T>(results: Vec<Option<T>>) -> anyhow::Result<Vec<T>> {
    let mut result: Vec<T> = Vec::with_capacity(results.len());

    for item_opt in results {
        if let Some(item) = item_opt {
            result.push(item);
        } else {
            return Err(anyhow::anyhow!("Missing value in unwrapped Vec result!"));
        }
    }
    Ok(result)
}
pub fn unwrap_kv_result<T>(item_opt: Option<T>) -> anyhow::Result<T> {
    if let Some(item) = item_opt {
        Ok(item)
    } else {
        return Err(anyhow::anyhow!("Missing value in unwrapped Vec result!"));
    }
}

pub trait KVQStoreAdapterReader<S, K: KVQSerializable, V: KVQSerializable> {
    fn get_exact_if_exists(s: &S, key: &K) -> anyhow::Result<Option<V>>;
    fn get_exact(s: &S, key: &K) -> anyhow::Result<V>;
    fn get_many_exact(s: &S, keys: &[K]) -> anyhow::Result<Vec<V>>;

    fn get_fuzzy_range_leq_kv(s: &S, key: &K, fuzzy_bytes: usize) -> anyhow::Result<Vec<KVQPair<K, V>>>;
    fn get_leq(s: &S, key: &K, fuzzy_bytes: usize) -> anyhow::Result<Option<V>>;
    fn get_leq_kv(s: &S, key: &K, fuzzy_bytes: usize) -> anyhow::Result<Option<KVQPair<K, V>>>;

    fn get_many_leq(s: &S, keys: &[K], fuzzy_bytes: usize) -> anyhow::Result<Vec<Option<V>>>;
    fn get_many_leq_kv(
        s: &S,
        keys: &[K],
        fuzzy_bytes: usize,
    ) -> anyhow::Result<Vec<Option<KVQPair<K, V>>>>;

    fn get_many_leq_u(s: &S, keys: &[K], fuzzy_bytes: usize) -> anyhow::Result<Vec<V>> {
        let results = Self::get_many_leq(s, keys, fuzzy_bytes)?;
        unwrap_kv_vec_result(results)
    }
    fn get_many_leq_kv_u(
        s: &S,
        keys: &[K],
        fuzzy_bytes: usize,
    ) -> anyhow::Result<Vec<KVQPair<K, V>>> {
        let results = Self::get_many_leq_kv(s, keys, fuzzy_bytes)?;
        unwrap_kv_vec_result(results)
    }
}

pub trait KVQStoreAdapter<S, K: KVQSerializable, V: KVQSerializable>:
    KVQStoreAdapterReader<S, K, V>
{
    fn set(s: &mut S, key: K, value: V) -> anyhow::Result<()>;
    fn set_ref(s: &mut S, key: &K, value: &V) -> anyhow::Result<()>;
    fn set_many_ref<'a>(s: &mut S, items: &[KVQPair<&'a K, &'a V>]) -> anyhow::Result<()>;
    fn set_many_split_ref(s: &mut S, keys: &[K], values: &[V]) -> anyhow::Result<()>;
    fn set_many(s: &mut S, items: &[KVQPair<K, V>]) -> anyhow::Result<()>;

    fn delete(s: &mut S, key: &K) -> anyhow::Result<bool>;
    fn delete_many(s: &mut S, keys: &[K]) -> anyhow::Result<Vec<bool>>;
    //fn delete_many_sized<const SIZE: usize>(s: &mut S, keys: &[K; SIZE]) ->
    // anyhow::Result<[bool; SIZE]>;
}

pub trait KVQStoreAdapterWithHelpers<S, K: KVQSerializable, V: KVQSerializable>:
    KVQStoreAdapter<S, K, V>
{
    fn set_many_ref_clone_batch<'a>(
        s: &mut S,
        items: &[KVQPair<&'a K, &'a V>],
    ) -> anyhow::Result<()> {
        let mut items_owned = Vec::with_capacity(items.len());
        for item in items {
            items_owned.push(KVQPair {
                key: item.key.clone(),
                value: item.value.clone(),
            });
        }
        Self::set_many(s, &items_owned)
    }
    fn set_many_ref_serial<'a>(s: &mut S, items: &[KVQPair<&'a K, &'a V>]) -> anyhow::Result<()> {
        for item in items {
            Self::set(s, item.key.clone(), item.value.clone())?;
        }
        Ok(())
    }
}

//pub type KVQStoreAdapter<K: KVQSerializable, V: KVQSerializable> =
// KVQStoreAdapter<KVQBinaryStore, K, V>;

pub trait KVQBinaryStoreReader {
    fn get_exact_if_exists(&self, key: &Vec<u8>) -> anyhow::Result<Option<Vec<u8>>>;
    fn get_exact(&self, key: &Vec<u8>) -> anyhow::Result<Vec<u8>>;
    fn get_many_exact(&self, keys: &[Vec<u8>]) -> anyhow::Result<Vec<Vec<u8>>>;

    fn get_leq(&self, key: &Vec<u8>, fuzzy_bytes: usize) -> anyhow::Result<Option<Vec<u8>>>;
    fn get_fuzzy_range_leq_kv(&self, key: &Vec<u8>, fuzzy_bytes: usize) -> anyhow::Result<Vec<KVQPair<Vec<u8>, Vec<u8>>>>;
    fn get_leq_kv(
        &self,
        key: &Vec<u8>,
        fuzzy_bytes: usize,
    ) -> anyhow::Result<Option<KVQPair<Vec<u8>, Vec<u8>>>>;

    fn get_many_leq(
        &self,
        keys: &[Vec<u8>],
        fuzzy_bytes: usize,
    ) -> anyhow::Result<Vec<Option<Vec<u8>>>>;
    fn get_many_leq_kv(
        &self,
        keys: &[Vec<u8>],
        fuzzy_bytes: usize,
    ) -> anyhow::Result<Vec<Option<KVQPair<Vec<u8>, Vec<u8>>>>>;

    fn get_leq_u(&self, key: &Vec<u8>, fuzzy_bytes: usize) -> anyhow::Result<Vec<u8>> {
        unwrap_kv_result(self.get_leq(key, fuzzy_bytes)?)
    }
    fn get_leq_kv_u(
        &self,
        key: &Vec<u8>,
        fuzzy_bytes: usize,
    ) -> anyhow::Result<KVQPair<Vec<u8>, Vec<u8>>> {
        unwrap_kv_result(self.get_leq_kv(key, fuzzy_bytes)?)
    }

    fn get_many_leq_u(&self, keys: &[Vec<u8>], fuzzy_bytes: usize) -> anyhow::Result<Vec<Vec<u8>>> {
        unwrap_kv_vec_result(self.get_many_leq(keys, fuzzy_bytes)?)
    }
    fn get_many_leq_kv_u(
        &self,
        keys: &[Vec<u8>],
        fuzzy_bytes: usize,
    ) -> anyhow::Result<Vec<KVQPair<Vec<u8>, Vec<u8>>>> {
        unwrap_kv_vec_result(self.get_many_leq_kv(keys, fuzzy_bytes)?)
    }
}

pub trait KVQBinaryStoreWriter {
    fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> anyhow::Result<()>;
    fn set_ref(&mut self, key: &Vec<u8>, value: &Vec<u8>) -> anyhow::Result<()>;
    fn set_many_ref<'a>(
        &mut self,
        items: &[KVQPair<&'a Vec<u8>, &'a Vec<u8>>],
    ) -> anyhow::Result<()>;
    fn set_many_vec(&mut self, items: Vec<KVQPair<Vec<u8>, Vec<u8>>>) -> anyhow::Result<()>;
    fn set_many_split_ref(&mut self, keys: &[Vec<u8>], values: &[Vec<u8>]) -> anyhow::Result<()>;

    fn delete(&mut self, key: &Vec<u8>) -> anyhow::Result<bool>;
    fn delete_many(&mut self, keys: &[Vec<u8>]) -> anyhow::Result<Vec<bool>>;
}


pub trait KVQBinaryStoreWriterImmutable {
    fn imm_set(&self, key: Vec<u8>, value: Vec<u8>) -> anyhow::Result<()>;
    fn imm_set_ref(&self, key: &Vec<u8>, value: &Vec<u8>) -> anyhow::Result<()>;
    fn imm_set_many_ref<'a>(
        &self,
        items: &[KVQPair<&'a Vec<u8>, &'a Vec<u8>>],
    ) -> anyhow::Result<()>;
    fn imm_set_many_vec(&self, items: Vec<KVQPair<Vec<u8>, Vec<u8>>>) -> anyhow::Result<()>;
    fn imm_set_many_split_ref(&self, keys: &[Vec<u8>], values: &[Vec<u8>]) -> anyhow::Result<()>;

    fn imm_delete(&self, key: &Vec<u8>) -> anyhow::Result<bool>;
    fn imm_delete_many(&self, keys: &[Vec<u8>]) -> anyhow::Result<Vec<bool>>;
}
pub trait KVQBinaryStoreWriterAutoImmutable: KVQBinaryStoreWriterImmutable {}
impl<T: KVQBinaryStoreWriterAutoImmutable> KVQBinaryStoreWriter for T {
    fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> anyhow::Result<()> {
        self.imm_set(key, value)
    }

    fn set_ref(&mut self, key: &Vec<u8>, value: &Vec<u8>) -> anyhow::Result<()> {
        self.imm_set_ref(key, value)
    }

    fn set_many_ref<'a>(
        &mut self,
        items: &[KVQPair<&'a Vec<u8>, &'a Vec<u8>>],
    ) -> anyhow::Result<()> {
        self.imm_set_many_ref(items)
    }

    fn set_many_vec(&mut self, items: Vec<KVQPair<Vec<u8>, Vec<u8>>>) -> anyhow::Result<()> {
        self.imm_set_many_vec(items)
    }

    fn set_many_split_ref(&mut self, keys: &[Vec<u8>], values: &[Vec<u8>]) -> anyhow::Result<()> {
        self.imm_set_many_split_ref(keys, values)
    }

    fn delete(&mut self, key: &Vec<u8>) -> anyhow::Result<bool> {
        self.imm_delete(key)
    }

    fn delete_many(&mut self, keys: &[Vec<u8>]) -> anyhow::Result<Vec<bool>> {
        self.imm_delete_many(keys)
    }
}


pub trait KVQBinaryStore: KVQBinaryStoreReader + KVQBinaryStoreWriter {}
pub trait KVQBinaryStoreImmutable: KVQBinaryStore + KVQBinaryStoreWriterImmutable {}

impl<T: KVQBinaryStoreReader + KVQBinaryStoreWriter> KVQBinaryStore for T {}
