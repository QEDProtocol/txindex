use std::collections::BTreeMap;
use std::ops::Bound::Included;

use crate::traits::KVQBinaryStoreReader;
use crate::traits::KVQBinaryStoreWriter;
use crate::traits::KVQPair;

pub struct KVQSimpleMemoryBackingStore {
    map: BTreeMap<Vec<u8>, Vec<u8>>,
}
impl KVQSimpleMemoryBackingStore {
    pub fn new() -> Self {
        Self {
            map: BTreeMap::new(),
        }
    }
}

impl KVQBinaryStoreReader for KVQSimpleMemoryBackingStore {
    fn get_exact(&self, key: &Vec<u8>) -> anyhow::Result<Vec<u8>> {
        match self.map.get(key) {
            Some(v) => Ok(v.to_owned()),
            None => anyhow::bail!("Key {} not found", hex::encode(&key)),
        }
    }

    fn get_many_exact(&self, keys: &[Vec<u8>]) -> anyhow::Result<Vec<Vec<u8>>> {
        let mut result = Vec::new();
        for key in keys {
            let r = self.get_exact(key)?;
            result.push(r);
        }
        Ok(result)
    }

    fn get_leq(&self, key: &Vec<u8>, fuzzy_bytes: usize) -> anyhow::Result<Option<Vec<u8>>> {
        let key_end = key.to_vec();
        let mut base_key = key.to_vec();
        let key_len = base_key.len();
        if fuzzy_bytes > key_len {
            return Err(anyhow::anyhow!(
                "Fuzzy bytes must be less than or equal to key length"
            ));
        }

        let mut sum_end = 0u32;
        for i in 0..fuzzy_bytes {
            sum_end += key_end[key_len - i - 1] as u32;
            base_key[key_len - i - 1] = 0;
        }

        if sum_end == 0 {
            let res = self.map.get(key);
            if res.is_none() {
                Ok(None)
            } else {
                Ok(Some(res.unwrap().to_owned()))
            }
        } else {
            let rq = self
                .map
                .range((Included(base_key), Included(key_end)))
                .next_back();

            if let Some((_, p)) = rq {
                Ok(Some(p.to_owned()))
            } else {
                Ok(None)
            }
        }
    }

    fn get_leq_kv(
        &self,
        key: &Vec<u8>,
        fuzzy_bytes: usize,
    ) -> anyhow::Result<Option<KVQPair<Vec<u8>, Vec<u8>>>> {
        let key_end = key.to_vec();
        let mut base_key = key.to_vec();
        let key_len = base_key.len();
        if fuzzy_bytes > key_len {
            return Err(anyhow::anyhow!(
                "Fuzzy bytes must be less than or equal to key length"
            ));
        }

        for i in 0..fuzzy_bytes {
            base_key[key_len - i - 1] = 0;
        }
        let rq = self
            .map
            .range((Included(base_key), Included(key_end)))
            .next_back();

        if let Some((k, v)) = rq {
            Ok(Some(KVQPair {
                key: k.to_owned(),
                value: v.to_owned(),
            }))
        } else {
            Ok(None)
        }
    }

    fn get_many_leq(
        &self,
        keys: &[Vec<u8>],
        fuzzy_bytes: usize,
    ) -> anyhow::Result<Vec<Option<Vec<u8>>>> {
        let mut results: Vec<Option<Vec<u8>>> = Vec::with_capacity(keys.len());
        for k in keys {
            let r = self.get_leq(k, fuzzy_bytes)?;
            results.push(r.to_owned());
        }
        Ok(results)
    }

    fn get_many_leq_kv(
        &self,
        keys: &[Vec<u8>],
        fuzzy_bytes: usize,
    ) -> anyhow::Result<Vec<Option<KVQPair<Vec<u8>, Vec<u8>>>>> {
        let mut results: Vec<Option<KVQPair<Vec<u8>, Vec<u8>>>> = Vec::with_capacity(keys.len());
        for k in keys {
            let r = self.get_leq_kv(k, fuzzy_bytes)?;
            results.push(r);
        }
        Ok(results)
    }

    fn get_exact_if_exists(&self, key: &Vec<u8>) -> anyhow::Result<Option<Vec<u8>>> {
        let result = self.map.get(key);
        if result.is_some() {
            Ok(Some(result.unwrap().to_owned()))
        } else {
            Ok(None)
        }
    }
    /*

    fn get_range_kv(
        &self,
        min_included: &Vec<u8>,
        max_included: &Vec<u8>,
    ) -> anyhow::Result<Vec<KVQPair<Vec<u8>, Vec<u8>>>> {
        let rq = self
            .map
            .range((
                Included(min_included.to_vec()),
                Included(max_included.to_vec()),
            ))
            .map(|(k, v)| KVQPair {
                key: k.to_owned(),
                value: v.to_owned(),
            })
            .collect::<Vec<_>>();
        Ok(rq)
    }

    fn get_prefix_range_kv(
        &self,
        prefix: &Vec<u8>,
        fuzzy_bytes: usize,
    ) -> anyhow::Result<Vec<KVQPair<Vec<u8>, Vec<u8>>>> {
        let mut base_key = vec![0u8; prefix.len() + fuzzy_bytes];
        base_key[0..prefix.len()].copy_from_slice(prefix);

        let mut key_end = base_key.to_vec();
        for i in ((prefix.len() - fuzzy_bytes)..prefix.len()) {
            key_end[i] = 0xff;
        }
        Ok(self
            .map
            .range((Included(base_key), Included(key_end)))
            .map(|(k, v)| KVQPair {
                key: k.to_owned(),
                value: v.to_owned(),
            })
            .collect::<Vec<_>>())
    }*/
    
    fn get_fuzzy_range_leq_kv(&self, key: &Vec<u8>, fuzzy_bytes: usize) -> anyhow::Result<Vec<KVQPair<Vec<u8>, Vec<u8>>>> {
        let key_end = key.to_vec();
        let mut base_key = key.to_vec();
        let key_len = base_key.len();
        if fuzzy_bytes > key_len {
            return Err(anyhow::anyhow!(
                "Fuzzy bytes must be less than or equal to key length"
            ));
        }

        for i in 0..fuzzy_bytes {
            base_key[key_len - i - 1] = 0;
        }

        Ok(self
            .map
            .range((Included(base_key), Included(key_end)))
            .map(|(k, v)| KVQPair {
                key: k.to_owned(),
                value: v.to_owned(),
            })
            .collect::<Vec<_>>())
    }
}

impl KVQBinaryStoreWriter for KVQSimpleMemoryBackingStore {
    fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> anyhow::Result<()> {
        self.map.insert(key, value);
        Ok(())
    }

    fn set_ref(&mut self, key: &Vec<u8>, value: &Vec<u8>) -> anyhow::Result<()> {
        self.map.insert(key.clone(), value.clone());
        Ok(())
    }

    fn set_many_ref<'a>(
        &mut self,
        items: &[KVQPair<&'a Vec<u8>, &'a Vec<u8>>],
    ) -> anyhow::Result<()> {
        for item in items {
            self.map.insert(item.key.clone(), item.value.clone());
        }
        Ok(())
    }

    fn set_many_vec(&mut self, items: Vec<KVQPair<Vec<u8>, Vec<u8>>>) -> anyhow::Result<()> {
        for item in items {
            self.map.insert(item.key, item.value);
        }
        Ok(())
    }

    fn delete(&mut self, key: &Vec<u8>) -> anyhow::Result<bool> {
        match self.map.remove(key) {
            Some(_) => Ok(true),
            None => Ok(false),
        }
    }

    fn delete_many(&mut self, keys: &[Vec<u8>]) -> anyhow::Result<Vec<bool>> {
        let mut result = Vec::with_capacity(keys.len());
        for key in keys {
            let r = self.delete(key)?;
            result.push(r);
        }
        Ok(result)
    }

    fn set_many_split_ref(&mut self, keys: &[Vec<u8>], values: &[Vec<u8>]) -> anyhow::Result<()> {
        if keys.len() != values.len() {
            anyhow::bail!("Keys and values must have the same length");
        } else {
            for i in 0..keys.len() {
                self.map.insert(keys[i].clone(), values[i].clone());
            }
            Ok(())
        }
    }
}
