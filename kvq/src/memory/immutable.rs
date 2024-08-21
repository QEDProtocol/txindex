use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::traits::{KVQBinaryStore, KVQBinaryStoreImmutable, KVQBinaryStoreReader, KVQBinaryStoreWriter, KVQBinaryStoreWriterImmutable, KVQPair};


pub struct KVQImmutableStoreWrapper<KVQ: KVQBinaryStore> {
    pub inner: RwLock<KVQ>,
}
impl<KVQ: KVQBinaryStore> KVQImmutableStoreWrapper<KVQ> {
    pub fn new(inner: KVQ) -> Self {
        Self {
            inner: RwLock::new(inner),
        }
    }

    pub fn write(&self) -> anyhow::Result<RwLockWriteGuard<KVQ>> {
        self.inner
            .try_write()
            .map_err(|err| anyhow::anyhow!("Error writing to immutable store: {:?}", err))
    }
    pub fn read(&self) -> anyhow::Result<RwLockReadGuard<KVQ>> {
        self.inner
            .try_read()
            .map_err(|err| anyhow::anyhow!("Error reading from immutable store: {:?}", err))
    }
}

impl<KVQ: KVQBinaryStore> KVQBinaryStoreReader for KVQImmutableStoreWrapper<KVQ> {
    fn get_exact_if_exists(&self, key: &Vec<u8>) -> anyhow::Result<Option<Vec<u8>>> {
        {
            self.read()?.get_exact_if_exists(key)
        }
    }

    fn get_exact(&self, key: &Vec<u8>) -> anyhow::Result<Vec<u8>> {
        {
            self.read()?.get_exact(key)
        }
    }

    fn get_many_exact(&self, keys: &[Vec<u8>]) -> anyhow::Result<Vec<Vec<u8>>> {
        {
            self.read()?.get_many_exact(keys)
        }
    }

    fn get_leq(&self, key: &Vec<u8>, fuzzy_bytes: usize) -> anyhow::Result<Option<Vec<u8>>> {
        {
            self.read()?.get_leq(key, fuzzy_bytes)
        }
    }

    fn get_fuzzy_range_leq_kv(
        &self,
        key: &Vec<u8>,
        fuzzy_bytes: usize,
    ) -> anyhow::Result<Vec<KVQPair<Vec<u8>, Vec<u8>>>> {
        {
            self.read()?.get_fuzzy_range_leq_kv(key, fuzzy_bytes)
        }
    }

    fn get_leq_kv(
        &self,
        key: &Vec<u8>,
        fuzzy_bytes: usize,
    ) -> anyhow::Result<Option<KVQPair<Vec<u8>, Vec<u8>>>> {
        {
            self.read()?.get_leq_kv(key, fuzzy_bytes)
        }
    }

    fn get_many_leq(
        &self,
        keys: &[Vec<u8>],
        fuzzy_bytes: usize,
    ) -> anyhow::Result<Vec<Option<Vec<u8>>>> {
        {
            self.read()?.get_many_leq(keys, fuzzy_bytes)
        }
    }

    fn get_many_leq_kv(
        &self,
        keys: &[Vec<u8>],
        fuzzy_bytes: usize,
    ) -> anyhow::Result<Vec<Option<KVQPair<Vec<u8>, Vec<u8>>>>> {
        {
            self.read()?.get_many_leq_kv(keys, fuzzy_bytes)
        }
    }
}

impl<KVQ: KVQBinaryStore> KVQBinaryStoreWriter for KVQImmutableStoreWrapper<KVQ> {
    fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> anyhow::Result<()> {
        {
            self.write()?.set(key, value)
        }
    }

    fn set_ref(&mut self, key: &Vec<u8>, value: &Vec<u8>) -> anyhow::Result<()> {
        {
            self.write()?.set_ref(key, value)
        }
    }

    fn set_many_ref<'a>(
        &mut self,
        items: &[KVQPair<&'a Vec<u8>, &'a Vec<u8>>],
    ) -> anyhow::Result<()> {
        {
            self.write()?.set_many_ref(items)
        }
    }

    fn set_many_vec(
        &mut self,
        items: Vec<KVQPair<Vec<u8>, Vec<u8>>>,
    ) -> anyhow::Result<()> {
        {
            self.write()?.set_many_vec(items)
        }
    }

    fn set_many_split_ref(&mut self, keys: &[Vec<u8>], values: &[Vec<u8>]) -> anyhow::Result<()> {
        {
            self.write()?.set_many_split_ref(keys, values)
        }
    }

    fn delete(&mut self, key: &Vec<u8>) -> anyhow::Result<bool> {
        {
            self.write()?.delete(key)
        }
    }

    fn delete_many(&mut self, keys: &[Vec<u8>]) -> anyhow::Result<Vec<bool>> {
        {
            self.write()?.delete_many(keys)
        }
    }
}


impl<KVQ: KVQBinaryStore> KVQBinaryStoreWriterImmutable for KVQImmutableStoreWrapper<KVQ> {
    fn imm_set(&self, key: Vec<u8>, value: Vec<u8>) -> anyhow::Result<()> {
        {
            self.write()?.set(key, value)
        }
    }

    fn imm_set_ref(&self, key: &Vec<u8>, value: &Vec<u8>) -> anyhow::Result<()> {
        {
            self.write()?.set_ref(key, value)
        }
    }

    fn imm_set_many_ref<'a>(
        &self,
        items: &[KVQPair<&'a Vec<u8>, &'a Vec<u8>>],
    ) -> anyhow::Result<()> {
        {
            self.write()?.set_many_ref(items)
        }
    }

    fn imm_set_many_vec(&self, items: Vec<KVQPair<Vec<u8>, Vec<u8>>>) -> anyhow::Result<()> {
        {
            self.write()?.set_many_vec(items)
        }
    }

    fn imm_set_many_split_ref(&self, keys: &[Vec<u8>], values: &[Vec<u8>]) -> anyhow::Result<()> {
        {
            self.write()?.set_many_split_ref(keys, values)
        }
    }

    fn imm_delete(&self, key: &Vec<u8>) -> anyhow::Result<bool> {
        {
            self.write()?.delete(key)
        }
    }

    fn imm_delete_many(&self, keys: &[Vec<u8>]) -> anyhow::Result<Vec<bool>> {
        {
            self.write()?.delete_many(keys)
        }
    }
}

impl<KVQ: KVQBinaryStore> KVQBinaryStoreImmutable for KVQImmutableStoreWrapper<KVQ> {}