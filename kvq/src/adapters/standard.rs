use std::marker::PhantomData;

use crate::traits::KVQBinaryStore;
use crate::traits::KVQBinaryStoreReader;
use crate::traits::KVQPair;
use crate::traits::KVQSerializable;
use crate::traits::KVQStoreAdapter;
use crate::traits::KVQStoreAdapterReader;

pub struct KVQStandardAdapter<S, K: KVQSerializable, V: KVQSerializable> {
    _s: PhantomData<S>,
    _k: PhantomData<K>,
    _v: PhantomData<V>,
}

impl<S: KVQBinaryStoreReader, K: KVQSerializable, V: KVQSerializable> KVQStoreAdapterReader<S, K, V>
    for KVQStandardAdapter<S, K, V>
{
    fn get_exact_if_exists(s: &S, key: &K) -> anyhow::Result<Option<V>> {
        let r = s.get_exact_if_exists(&key.to_bytes()?)?;
        if r.is_some() {
            let result = V::from_bytes(&r.unwrap())?;
            Ok(Some(result))
        } else {
            Ok(None)
        }
    }
    fn get_exact(s: &S, key: &K) -> anyhow::Result<V> {
        let r = s.get_exact(&key.to_bytes()?)?;
        Ok(V::from_bytes(&r)?)
    }

    fn get_leq_kv(s: &S, key: &K, fuzzy_bytes: usize) -> anyhow::Result<Option<KVQPair<K, V>>> {
        let r = s.get_leq_kv(&key.to_bytes()?, fuzzy_bytes)?;
        match r {
            Some(kv) => Ok(Some(KVQPair {
                key: K::from_bytes(&kv.key)?,
                value: V::from_bytes(&kv.value)?,
            })),
            None => Ok(None),
        }
    }

    fn get_many_exact(s: &S, keys: &[K]) -> anyhow::Result<Vec<V>> {
        let keys_bytes = keys
            .iter()
            .map(|k| k.to_bytes())
            .collect::<anyhow::Result<Vec<Vec<u8>>>>()?;
        let values_bytes = s.get_many_exact(&keys_bytes)?;
        let values = values_bytes
            .iter()
            .map(|r| V::from_bytes(r))
            .collect::<anyhow::Result<Vec<V>>>();
        Ok(values?)
    }

    fn get_leq(s: &S, key: &K, fuzzy_bytes: usize) -> anyhow::Result<Option<V>> {
        let r = s.get_leq(&key.to_bytes()?, fuzzy_bytes)?;
        match r {
            Some(v) => Ok(Some(V::from_bytes(&v)?)),
            None => Ok(None),
        }
    }

    fn get_many_leq(s: &S, keys: &[K], fuzzy_bytes: usize) -> anyhow::Result<Vec<Option<V>>> {
        let keys_bytes = keys
            .iter()
            .map(|k| k.to_bytes())
            .collect::<anyhow::Result<Vec<Vec<u8>>>>()?;
        let values_bytes = s.get_many_leq(&keys_bytes, fuzzy_bytes)?;
        let values = values_bytes
            .iter()
            .map(|r| {
                Ok(match r {
                    Some(v) => Some(V::from_bytes(v)?),
                    None => None,
                })
            })
            .collect::<anyhow::Result<Vec<Option<V>>>>();
        Ok(values?)
    }

    fn get_many_leq_kv(
        s: &S,
        keys: &[K],
        fuzzy_bytes: usize,
    ) -> anyhow::Result<Vec<Option<KVQPair<K, V>>>> {
        let keys_bytes = keys
            .iter()
            .map(|k| k.to_bytes())
            .collect::<anyhow::Result<Vec<Vec<u8>>>>()?;
        let kvs_bytes = s.get_many_leq_kv(&keys_bytes, fuzzy_bytes)?;
        let kvs: anyhow::Result<Vec<Option<KVQPair<K, V>>>> = kvs_bytes
            .iter()
            .map(|r| {
                Ok(match r {
                    Some(kv) => Some(KVQPair {
                        key: K::from_bytes(&kv.key)?,
                        value: V::from_bytes(&kv.value)?,
                    }),
                    None => None,
                })
            })
            .collect();
        Ok(kvs?)
    }

    fn get_fuzzy_range_leq_kv(
        s: &S,
        key: &K,
        fuzzy_bytes: usize,
    ) -> anyhow::Result<Vec<KVQPair<K, V>>> {
        let key = key.to_bytes()?;
        s.get_fuzzy_range_leq_kv(&key, fuzzy_bytes)?
            .into_iter()
            .map(|kv| {
                Ok(KVQPair {
                    key: K::from_bytes(&kv.key)?,
                    value: V::from_bytes(&kv.value)?,
                })
            })
            .collect()
    }
}

impl<S: KVQBinaryStore, K: KVQSerializable, V: KVQSerializable> KVQStoreAdapter<S, K, V>
    for KVQStandardAdapter<S, K, V>
{
    fn set_ref(s: &mut S, key: &K, value: &V) -> anyhow::Result<()> {
        s.set(key.to_bytes()?, value.to_bytes()?)
    }
    fn set(s: &mut S, key: K, value: V) -> anyhow::Result<()> {
        s.set(key.to_bytes()?, value.to_bytes()?)
    }

    fn set_many_ref<'a>(s: &mut S, items: &[KVQPair<&'a K, &'a V>]) -> anyhow::Result<()> {
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

    fn set_many(s: &mut S, items: &[KVQPair<K, V>]) -> anyhow::Result<()> {
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

    fn delete(s: &mut S, key: &K) -> anyhow::Result<bool> {
        s.delete(&key.to_bytes()?)
    }

    fn delete_many(s: &mut S, keys: &[K]) -> anyhow::Result<Vec<bool>> {
        let mut results: Vec<bool> = Vec::with_capacity(keys.len());

        for k in keys {
            let r = s.delete(&k.to_bytes()?)?;
            results.push(r)
        }
        Ok(results)
    }

    fn set_many_split_ref(s: &mut S, keys: &[K], values: &[V]) -> anyhow::Result<()> {
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
