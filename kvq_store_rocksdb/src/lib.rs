use std::path::Path;
use std::sync::Arc;

use kvq::traits::KVQBinaryStoreImmutable;
use kvq::traits::KVQBinaryStoreReader;
use kvq::traits::KVQBinaryStoreWriterAutoImmutable;
use kvq::traits::KVQBinaryStoreWriterImmutable;
use kvq::traits::KVQPair;
use rocksdb::ErrorKind;
pub mod compat;
#[derive(Clone)]
pub struct KVQRocksDBStore {
    db: Arc<rocksdb::DB>,
}
impl KVQRocksDBStore {
    pub fn open_default<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {

        let mut db_opts = rocksdb::Options::default();
        db_opts.create_if_missing(true);
        db_opts.set_max_open_files(100_000); // TODO: make sure to `ulimit -n` this process correctly
        db_opts.set_compaction_style(rocksdb::DBCompactionStyle::Level);
        db_opts.set_compression_type(rocksdb::DBCompressionType::Snappy);
        db_opts.set_target_file_size_base(1_073_741_824);
        db_opts.set_write_buffer_size(256 << 20);
        db_opts.set_disable_auto_compactions(false); // for initial bulk load

        // db_opts.set_advise_random_on_open(???);
        db_opts.set_compaction_readahead_size(1 << 20);
        db_opts.increase_parallelism(2);

        // let mut block_opts = rocksdb::BlockBasedOptions::default();
        // block_opts.set_block_size(???);
        let db_inner = rocksdb::DB::open(&db_opts, path).expect("failed to open RocksDB");

        let db = Self {
            db: Arc::new(db_inner),
        };
        Ok(db)
    }
}
fn compare_u8_array_a_le_b(a: &[u8], b: &[u8]) -> bool {
    return a <= b;
    /*
    if a.len() != b.len() {
        return a.len() < b.len();
    }else{
        for i in 0..a.len() {
            if a[i] != b[i] {
                return a[i] < b[i];
            }
        }
        return false;
    }*/


}

impl KVQBinaryStoreReader for KVQRocksDBStore {
    fn get_exact(&self, key: &Vec<u8>) -> anyhow::Result<Vec<u8>> {
        match self.db.get(key)? {
            Some(v) => Ok(v),
            None => anyhow::bail!("Key not found"),
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

        for i in 0..fuzzy_bytes {
            base_key[key_len - i - 1] = 0;
        }

        let rq = self
            .db
            .prefix_iterator(base_key)
            .take_while(|v| match v {
                Ok((k, _)) if compare_u8_array_a_le_b(k.as_ref(), &key_end) => true,
                _ => false,
            })
            .last();

        match rq {
            Some(Ok((_, v))) => Ok(Some(v.to_vec())),
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
            .db
            .prefix_iterator(base_key)
            .take_while(|v| match v {
                Ok((k, _)) if compare_u8_array_a_le_b(k.as_ref(), &key_end) => true,
                _ => false,
            })
            .last();

        match rq {
            Some(Ok((k, v))) => Ok(Some(KVQPair {
                key: k.to_vec(),
                value: v.to_vec(),
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
        let res = self.db.get(key.as_slice())?;
        if res.is_some() {
            Ok(Some(res.unwrap().to_vec()))
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
        self.db
            .prefix_iterator(base_key)
            .take_while(|v| match v {
                Ok((k, _)) if compare_u8_array_a_le_b(k.as_ref(), &key_end) => true,
                _ => false,
            })
            .map(|x| {
                let x = x?;
                Ok(KVQPair {
                    key: x.0.to_vec(),
                    value: x.1.to_vec(),
                })
            })
            .collect::<Result<Vec<_>, _>>()
    }
}
/*
impl KVQBinaryStoreWriter for KVQRocksDBStore {
    fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> anyhow::Result<()> {
        self.db.put(key, value)?;
        Ok(())
    }

    fn set_ref(&mut self, key: &Vec<u8>, value: &Vec<u8>) -> anyhow::Result<()> {
        self.db.put(key.clone(), value.clone())?;
        Ok(())
    }

    fn set_many_ref<'a>(
        &mut self,
        items: &[KVQPair<&'a Vec<u8>, &'a Vec<u8>>],
    ) -> anyhow::Result<()> {
        let txn = self.db.transaction();
        for item in items {
            txn.put(item.key.clone(), item.value.clone())?;
        }
        Ok(txn.commit()?)
    }

    fn set_many_vec(&mut self, items: Vec<KVQPair<Vec<u8>, Vec<u8>>>) -> anyhow::Result<()> {
        let txn = self.db.transaction();
        for item in items {
            txn.put(item.key, item.value)?;
        }
        Ok(txn.commit()?)
    }

    fn delete(&mut self, key: &Vec<u8>) -> anyhow::Result<bool> {
        match self.db.delete(key) {
            Ok(_) => Ok(true),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(true),
            Err(e) => anyhow::bail!(e),
        }
    }

    fn delete_many(&mut self, keys: &[Vec<u8>]) -> anyhow::Result<Vec<bool>> {
        let mut result = Vec::with_capacity(keys.len());
        let txn = self.db.transaction();
        for key in keys {
            let r = match txn.delete(key) {
                Ok(_) => true,
                Err(e) if e.kind() == ErrorKind::NotFound => true,
                Err(e) => anyhow::bail!(e),
            };
            result.push(r);
        }
        txn.commit()?;
        Ok(result)
    }

    fn set_many_split_ref(&mut self, keys: &[Vec<u8>], values: &[Vec<u8>]) -> anyhow::Result<()> {
        if keys.len() != values.len() {
            return Err(anyhow::anyhow!(
                "Keys and values must be of the same length"
            ));
        }
        let txn = self.db.transaction();
        for (k, v) in keys.iter().zip(values) {
            txn.put(k.as_slice(), v.as_slice())?;
        }
        Ok(txn.commit()?)
    }
}
*/
impl KVQBinaryStoreWriterImmutable for KVQRocksDBStore {
    fn imm_set(&self, key: Vec<u8>, value: Vec<u8>) -> anyhow::Result<()> {
        self.db.put(key, value)?;
        self.db.flush()?;

        Ok(())
    }

    fn imm_set_ref(&self, key: &Vec<u8>, value: &Vec<u8>) -> anyhow::Result<()> {
        self.db.put(key.clone(), value.clone())?;
        self.db.flush()?;

        Ok(())
    }

    fn imm_set_many_ref<'a>(
        &self,
        items: &[KVQPair<&'a Vec<u8>, &'a Vec<u8>>],
    ) -> anyhow::Result<()> {
        for item in items {
            self.db.put(item.key.clone(), item.value.clone())?;
        }
        self.db.flush()?;
        Ok(())
    }

    fn imm_set_many_vec(&self, items: Vec<KVQPair<Vec<u8>, Vec<u8>>>) -> anyhow::Result<()> {
        for item in items {
            self.db.put(item.key, item.value)?;
        }
        self.db.flush()?;

        Ok(())
    }

    fn imm_delete(&self, key: &Vec<u8>) -> anyhow::Result<bool> {
        match self.db.delete(key) {
            Ok(_) => Ok(true),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(true),
            Err(e) => anyhow::bail!(e),
        }
    }

    fn imm_delete_many(&self, keys: &[Vec<u8>]) -> anyhow::Result<Vec<bool>> {
        let mut result = Vec::with_capacity(keys.len());
        for key in keys {
            let r = match self.db.delete(key) {
                Ok(_) => true,
                Err(e) if e.kind() == ErrorKind::NotFound => true,
                Err(e) => anyhow::bail!(e),
            };
            result.push(r);
        }
        Ok(result)
    }

    fn imm_set_many_split_ref(&self, keys: &[Vec<u8>], values: &[Vec<u8>]) -> anyhow::Result<()> {
        if keys.len() != values.len() {
            return Err(anyhow::anyhow!(
                "Keys and values must be of the same length"
            ));
        }
        for (k, v) in keys.iter().zip(values) {
            self.db.put(k.as_slice(), v.as_slice())?;
        }
        self.db.flush()?;
        Ok(())
    }
}

impl KVQBinaryStoreWriterAutoImmutable for KVQRocksDBStore {}

impl KVQBinaryStoreImmutable for KVQRocksDBStore {}