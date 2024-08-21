use kvq::traits::KVQBinaryStoreReader;
use kvq::traits::KVQBinaryStoreWriter;
use kvq::traits::KVQPair;
use redb::ReadableTable;
use redb::Table;

pub struct KVQReDBStore<T> {
    kv: T,
}
impl<T> KVQReDBStore<T> {
    pub fn new(kv: T) -> Self {
        Self { kv }
    }
}

impl<T> KVQBinaryStoreReader for KVQReDBStore<T>
where
    T: ReadableTable<&'static [u8], &'static [u8]>,
{
    fn get_exact(&self, key: &Vec<u8>) -> anyhow::Result<Vec<u8>> {
        let res = self
            .kv
            .get(key.as_slice())?
            .ok_or(anyhow::anyhow!("Key not found"))?
            .value()
            .to_vec();
        Ok(res)
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

        for i in 0..fuzzy_bytes {
            base_key[key_len - i - 1] = 0;
        }

        let rq = self
            .kv
            .range(base_key.as_slice()..=key_end.as_slice())?
            .next_back();

        match rq {
            Some(Ok((_, v))) => Ok(Some(v.value().to_vec())),
            _ => Ok(None),
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
            .kv
            .range(base_key.as_slice()..=key_end.as_slice())?
            .next_back();

        match rq {
            Some(Ok((k, v))) => Ok(Some(KVQPair {
                key: k.value().to_vec(),
                value: v.value().to_vec(),
            })),
            _ => Ok(None),
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
            results.push(r);
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
        let res = self.kv.get(key.as_slice())?;
        if res.is_some() {
            Ok(Some(res.unwrap().value().to_vec()))
        } else {
            Ok(None)
        }
    }

    fn get_fuzzy_range_leq_kv(
        &self,
        key: &Vec<u8>,
        fuzzy_bytes: usize,
    ) -> anyhow::Result<Vec<KVQPair<Vec<u8>, Vec<u8>>>> {
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

        self.kv
            .range(base_key.as_slice()..=key_end.as_slice())?
            .map(|x| {
                let x = x?;
                Ok(KVQPair {
                    key: x.0.value().to_vec(),
                    value: x.1.value().to_vec(),
                })
            })
            .collect::<Result<Vec<_>, _>>()
    }
}

impl<'db, 'txn> KVQBinaryStoreWriter
    for KVQReDBStore<Table<'db, 'txn, &'static [u8], &'static [u8]>>
{
    fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> anyhow::Result<()> {
        self.set_ref(&key, &value)
    }

    fn set_ref(&mut self, key: &Vec<u8>, value: &Vec<u8>) -> anyhow::Result<()> {
        self.kv.insert(key.as_slice(), value.as_slice())?;
        Ok(())
    }

    fn set_many_ref(&mut self, items: &[KVQPair<&'_ Vec<u8>, &'_ Vec<u8>>]) -> anyhow::Result<()> {
        for item in items {
            self.kv.insert(item.key.as_slice(), item.value.as_slice())?;
        }
        Ok(())
    }

    fn set_many_vec(&mut self, items: Vec<KVQPair<Vec<u8>, Vec<u8>>>) -> anyhow::Result<()> {
        self.set_many_ref(
            items
                .iter()
                .map(|x| KVQPair {
                    key: &x.key,
                    value: &x.value,
                })
                .collect::<Vec<_>>()
                .as_slice(),
        )
    }

    fn delete(&mut self, key: &Vec<u8>) -> anyhow::Result<bool> {
        Ok(self.kv.remove(key.as_slice())?.is_some())
    }

    fn delete_many(&mut self, keys: &[Vec<u8>]) -> anyhow::Result<Vec<bool>> {
        let mut result = Vec::with_capacity(keys.len());
        for key in keys {
            let r = self.kv.remove(key.as_slice())?;
            result.push(r.is_some());
        }
        Ok(result)
    }

    fn set_many_split_ref(&mut self, keys: &[Vec<u8>], values: &[Vec<u8>]) -> anyhow::Result<()> {
        if keys.len() != values.len() {
            return Err(anyhow::anyhow!(
                "Keys and values must be of the same length"
            ));
        }
        for (k, v) in keys.iter().zip(values) {
            self.kv.insert(k.as_slice(), v.as_slice())?;
        }
        Ok(())
    }
}
