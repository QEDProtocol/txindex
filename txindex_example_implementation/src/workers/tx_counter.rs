use std::{marker::PhantomData, sync::Arc};

use bitcoin::{Block, Transaction};
use kvq::{cache::KVQBinaryStoreCached, traits::KVQBinaryStoreImmutable};
use txindex_common::{
    db::{chain::TxIndexChainAPI, indexed_block_db::IndexedBlockDBStore},
    worker::traits::TxIndexWorker,
};
use txindex_server::daemon::schema::compute_script_hash;

use crate::tables::tx_counter::SimpleTxCounterDB;

pub struct TxCounterWorker<KVQ: KVQBinaryStoreImmutable, T: TxIndexChainAPI> {
    pub _kvq: PhantomData<KVQ>,
    pub _chain: PhantomData<T>,
}
impl<KVQ: KVQBinaryStoreImmutable, T: TxIndexChainAPI> TxCounterWorker<KVQ, T> {
    fn process_tx(
        db: &mut IndexedBlockDBStore<KVQBinaryStoreCached<KVQ>>,
        _q: Arc<T>,
        _block_number: u64,
        _block: &Block,
        tx: &Transaction,
    ) -> anyhow::Result<()> {
        for input in tx.output.iter() {
            let hash = compute_script_hash(&input.script_pubkey);
            let ctr = db
                .get::<SimpleTxCounterDB>(&hash)?
                .or(Some(SimpleTxCounterDB { spend_count: 0 }))
                .unwrap();
            db.put::<SimpleTxCounterDB>(
                &hash,
                &SimpleTxCounterDB {
                    spend_count: ctr.spend_count + 1,
                },
            )?;
        }
        Ok(())
    }
}
impl<KVQ: KVQBinaryStoreImmutable, T: TxIndexChainAPI> TxIndexWorker<KVQ, T>
    for TxCounterWorker<KVQ, T>
{
    fn process_block(
        db: &mut IndexedBlockDBStore<KVQBinaryStoreCached<KVQ>>,
        q: Arc<T>,
        block_number: u64,
        block: &Block,
    ) -> anyhow::Result<()> {
        for tx in block.txdata.iter() {
            Self::process_tx(db, q.clone(), block_number, block, tx)?;
        }

        Ok(())
    }
}
