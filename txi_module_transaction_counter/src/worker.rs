use std::{marker::PhantomData, sync::Arc};

use bitcoin::{Block, Transaction};
use itertools::Itertools;
use kvq::{cache::KVQBinaryStoreCached, traits::KVQBinaryStoreImmutable};
use txindex_common::{
    db::{chain::TxIndexChainAPI, indexed_block_db::IndexedBlockDBStore}, utils::transaction::{get_input_addresses_for_transaction, get_output_addresses_for_transaction}, worker::traits::TxIndexWorker
};

use crate::{tables::SimpleTxCounterDB, utils::get_scriptpubkey_hash};


pub struct TxCounterWorker<KVQ: KVQBinaryStoreImmutable, T: TxIndexChainAPI> {
    pub _kvq: PhantomData<KVQ>,
    pub _chain: PhantomData<T>,
}
impl<KVQ: KVQBinaryStoreImmutable, T: TxIndexChainAPI> TxCounterWorker<KVQ, T> {
    fn process_tx(
        db: &mut IndexedBlockDBStore<KVQBinaryStoreCached<KVQ>>,
        q: Arc<T>,
        _block_number: u64,
        _block: &Block,
        tx: &Transaction,
    ) -> anyhow::Result<()> {
        let inputs = get_input_addresses_for_transaction::<T>(&q, tx).into_iter().unique().collect::<Vec<_>>();
        let outputs = get_output_addresses_for_transaction(tx, q.get_network()).into_iter().unique().collect::<Vec<_>>();
        for input in inputs.iter() {
            let hash = get_scriptpubkey_hash(&input.script_pubkey());
            let ctr = db
                .get::<SimpleTxCounterDB>(&hash)?
                .or(Some(SimpleTxCounterDB { spend_count: 0, receive_count: 0 }))
                .unwrap();
            db.put::<SimpleTxCounterDB>(
                &hash,
                &SimpleTxCounterDB {
                    spend_count: ctr.spend_count + 1,
                    receive_count: ctr.receive_count,
                },
            )?;
        }
        for output in outputs.iter() {
            let hash = get_scriptpubkey_hash(&output.script_pubkey());
            let ctr = db
                .get::<SimpleTxCounterDB>(&hash)?
                .or(Some(SimpleTxCounterDB { spend_count: 0, receive_count: 0 }))
                .unwrap();
            db.put::<SimpleTxCounterDB>(
                &hash,
                &SimpleTxCounterDB {
                    spend_count: ctr.spend_count,
                    receive_count: ctr.receive_count + 1,
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
