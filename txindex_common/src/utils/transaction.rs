use bitcoin::{address::NetworkChecked, hashes::Hash, Address, BlockHash, OutPoint, Transaction, TxIn, TxOut, Txid};
use serde::{Deserialize, Serialize};

use std::collections::HashMap;

use crate::{chain::Network, db::chain::TxIndexChainAPI};

use super::block::BlockId;


#[derive(Serialize, Deserialize, Debug)]
pub struct TransactionStatus {
    pub confirmed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block_height: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block_hash: Option<BlockHash>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block_time: Option<u32>,
}

impl From<Option<BlockId>> for TransactionStatus {
    fn from(blockid: Option<BlockId>) -> TransactionStatus {
        match blockid {
            Some(b) => TransactionStatus {
                confirmed: true,
                block_height: Some(b.height as usize),
                block_hash: Some(b.hash),
                block_time: Some(b.time),
            },
            None => TransactionStatus {
                confirmed: false,
                block_height: None,
                block_hash: None,
                block_time: None,
            },
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct TxInput {
    pub txid: Txid,
    pub vin: u16,
}

pub fn is_coinbase(txin: &TxIn) -> bool {
  return txin.previous_output.is_null();
}

pub fn has_prevout(txin: &TxIn) -> bool {
  return !txin.previous_output.is_null();
}

pub fn is_spendable(txout: &TxOut) -> bool {
  return !txout.script_pubkey.is_provably_unspendable();
}

pub fn extract_tx_prevouts<'a>(
    tx: &Transaction,
    txos: &'a HashMap<OutPoint, TxOut>,
    allow_missing: bool,
) -> HashMap<u32, &'a TxOut> {
    tx.input
        .iter()
        .enumerate()
        .filter(|(_, txi)| has_prevout(txi))
        .filter_map(|(index, txi)| {
            Some((
                index as u32,
                txos.get(&txi.previous_output).or_else(|| {
                    assert!(allow_missing, "missing outpoint {:?}", txi.previous_output);
                    None
                })?,
            ))
        })
        .collect()
}

pub fn serialize_outpoint<S>(outpoint: &OutPoint, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::ser::Serializer,
{
    use serde::ser::SerializeStruct;
    let mut s = serializer.serialize_struct("OutPoint", 2)?;
    s.serialize_field("txid", &outpoint.txid)?;
    s.serialize_field("vout", &outpoint.vout)?;
    s.end()
}

pub fn get_input_addresses_for_transaction<Q: TxIndexChainAPI>(chain: &Q, tx: &Transaction) -> Vec<Address<NetworkChecked>> {
    let n = chain.get_bitcoin_network();
    tx.input.iter().filter_map(|txin| {
        if is_coinbase(txin) {
            return None;
        }
        let prevout = chain.get_transaction(txin.previous_output.txid.as_raw_hash().to_byte_array()).ok()?;
        let k = prevout.output.get(txin.previous_output.vout as usize);
        if k.is_none() {
            return None;
        }
        let address = Address::from_script(&k.unwrap().script_pubkey, n).map(|x| Some(x)).unwrap_or(None);
        if address.is_none() {
            return None;
        }else{
            return address;
        }
    }).collect()
}
pub fn get_output_addresses_for_transaction(tx: &Transaction, network: Network) -> Vec<Address<NetworkChecked>> {
    tx.output.iter().filter_map(|txout| {
         let p = Address::from_script(&txout.script_pubkey, network.into());
         if p.is_err() {
              None
         }else{
            Some(p.unwrap())
         }
    }).collect()
}