use std::{collections::{BTreeSet, HashMap}, sync::Arc};

use bitcoin::{BlockHash, OutPoint, Transaction, TxOut, Txid};
use kvq::{base_types::{DBFlush, DBRow}, cache::KVQBinaryStoreCached};
use log::{debug, info};
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use txindex_common::{chain::Network, config::Config, db::{chain::TxIndexChainAPI, indexed_block::IndexedBlockFull, indexed_block_db::IndexedBlockDBStore, kvstore::{BaseCDBStore, BaseKVQStore, TxIndexStore}}, utils::{block::{BlockEntry, BlockMeta, HeaderEntry}, full_hash, transaction::{has_prevout, is_spendable}}, worker::traits::TxIndexWorker};

use crate::{daemon::fetcher::start_fetcher, db::IndexForkHelper, utils::metrics::{Gauge, HistogramOpts, HistogramTimer, HistogramVec, MetricOpts, Metrics}};

use super::{daemon::Daemon, fetcher::FetchFrom, schema::{addr_search_row, BlockRow, ChainQuery, FundingInfo, GetAmountVal, SpendingInfo, TxConfRow, TxEdgeRow, TxHistoryInfo, TxHistoryRow, TxOutRow, TxRow}};
use bitcoin::consensus::encode::{deserialize, serialize};

use txindex_errors::core::*;
pub type FullHash = [u8; 32]; // serialized SHA256 result




pub struct Indexer {
  store: Arc<TxIndexStore>,
  flush: DBFlush,
  from: FetchFrom,
  iconfig: IndexerConfig,
  duration: HistogramVec,
  tip_metric: Gauge,
}

pub struct IndexerConfig {
  light_mode: bool,
  address_search: bool,
  index_unspendables: bool,
  network: Network,
}

// TODO: &[Block] should be an iterator / a queue.
impl Indexer {
  pub fn open(store: Arc<TxIndexStore>, from: FetchFrom, config: &Config, metrics: &Metrics) -> Self {
      Indexer {
          store,
          flush: DBFlush::Disable,
          from,
          iconfig: IndexerConfig::from(config),
          duration: metrics.histogram_vec(
              HistogramOpts::new("index_duration", "Index update duration (in seconds)"),
              &["step"],
          ),
          tip_metric: metrics.gauge(MetricOpts::new("tip_height", "Current chain tip height")),
      }
  }

  fn start_timer(&self, name: &str) -> HistogramTimer {
      self.duration.with_label_values(&[name]).start_timer()
  }

  fn headers_to_add(&self, new_headers: &[HeaderEntry]) -> Vec<HeaderEntry> {
      let added_blockhashes = self.store.added_blockhashes.read().unwrap();
      new_headers
          .iter()
          .filter(|e| !added_blockhashes.contains(e.hash()))
          .cloned()
          .collect()
  }

  fn headers_to_index(&self, new_headers: &[HeaderEntry]) -> Vec<HeaderEntry> {
      let indexed_blockhashes = self.store.indexed_blockhashes.read().unwrap();
      new_headers
          .iter()
          .filter(|e| !indexed_blockhashes.contains(e.hash()))
          .cloned()
          .collect()
  }

  fn start_auto_compactions(&self, db: &BaseCDBStore) {

      let key = b"F".to_vec();
      if db.get(&key).is_none() {
          db.full_compaction();
          db.put_sync(&key, b"");
          assert!(db.get(&key).is_some());
      }
      db.enable_auto_compaction();
  }

  fn get_new_headers(&self, daemon: &Daemon, tip: &BlockHash) -> Result<Vec<HeaderEntry>> {
      let headers = self.store.indexed_headers.read().unwrap();
      let new_headers = daemon.get_new_headers(&headers, &tip)?;
      let result = headers.order(new_headers);

      if let Some(tip) = result.last() {
          info!("{:?} ({} left to index)", tip, result.len());
      };
      Ok(result)
  }

  pub fn update<I: TxIndexWorker<BaseKVQStore, Q>, Q: TxIndexChainAPI>(&mut self, daemon: &Daemon, q: Arc<Q>) -> Result<BlockHash> {
      let daemon = daemon.reconnect()?;
      let tip = daemon.getbestblockhash()?;
      let new_headers = self.get_new_headers(&daemon, &tip)?;

      let to_add = self.headers_to_add(&new_headers);
      debug!(
          "adding transactions from {} blocks using {:?}",
          to_add.len(),
          self.from
      );
      start_fetcher(self.from, &daemon, to_add)?.map(|blocks| self.add(&blocks));
      self.start_auto_compactions(&self.store.txstore_db);
      let to_index = self.headers_to_index(&new_headers);
      debug!(
          "indexing history from {} blocks using {:?}",
          to_index.len(),
          self.from
      );
      start_fetcher(self.from, &daemon, to_index)?.map(|blocks| self.index::<I, Q>(Arc::clone(&q), &blocks));
      self.start_auto_compactions(&self.store.history_db);

      if let DBFlush::Disable = self.flush {
          debug!("flushing to disk");
          self.store.txstore_db.flush();
          self.store.history_db.flush();
          self.flush = DBFlush::Enable;
      }
      // update the synced tip *after* the new data is flushed to disk
      debug!("updating synced tip to {:?}", tip);
      self.store.txstore_db.put_sync(b"t", &serialize(&tip));

      let mut headers = self.store.indexed_headers.write().unwrap();
      headers.apply(new_headers);
      assert_eq!(tip, *headers.tip());

      if let FetchFrom::BlkFiles = self.from {
          self.from = FetchFrom::Bitcoind;
      }

      self.tip_metric.set(headers.len() as i64 - 1);
      Ok(tip)
  }

  fn add(&self, blocks: &[BlockEntry]) {
      // TODO: skip orphaned blocks?
      let rows = {
          let _timer = self.start_timer("add_process");
          add_blocks(blocks, &self.iconfig)
      };
      {
          let _timer = self.start_timer("add_write");
          self.store.txstore_db.write(rows, self.flush);
      }

      self.store
          .added_blockhashes
          .write()
          .unwrap()
          .extend(blocks.iter().map(|b| b.entry.hash()));
  }

  fn index<I: TxIndexWorker<BaseKVQStore, Q>, Q: TxIndexChainAPI>(&self, q: Arc<Q>, blocks: &[BlockEntry]) {
      let previous_txos_map = {
          let _timer = self.start_timer("index_lookup");
          lookup_txos(&self.store.txstore_db, &get_previous_txos(blocks), false)
      };
      let rows = {
          let _timer = self.start_timer("index_process");
          let added_blockhashes = self.store.added_blockhashes.read().unwrap();
          for b in blocks {
              let blockhash = b.entry.hash();
              // TODO: replace by lookup into txstore_db?
              if !added_blockhashes.contains(blockhash) {
                  panic!("cannot index block {} (missing from store)", blockhash);
              }
          }
          index_blocks(blocks, &previous_txos_map, &self.iconfig)
      };
      
      rows.into_iter().zip(blocks).for_each(|(r, b)|{
        let mut ibdb = IndexedBlockDBStore::new_from_block(KVQBinaryStoreCached::new(Arc::clone(&self.store.indexer_db)), b.entry.height() as u64, &b.block);

        IndexForkHelper::<BaseKVQStore, Q, I>::update_with_block(&mut ibdb, Arc::clone(&q), b.entry.height() as u64, &b.block).unwrap();
        IndexedBlockFull::save_from_db_store(ibdb).unwrap();
        self.store.history_db.write(r, self.flush);

      });
  }

  pub fn fetch_from(&mut self, from: FetchFrom) {
      self.from = from;
  }
}


impl From<&Config> for IndexerConfig {
  fn from(config: &Config) -> Self {
      IndexerConfig {
          light_mode: config.light_mode,
          address_search: config.address_search,
          index_unspendables: config.index_unspendables,
          network: config.network_type,
          #[cfg(feature = "liquid")]
          parent_network: config.parent_network,
      }
  }
}

fn add_blocks(block_entries: &[BlockEntry], iconfig: &IndexerConfig) -> Vec<DBRow> {
  // persist individual transactions:
  //      T{txid} → {rawtx}
  //      C{txid}{blockhash}{height} →
  //      O{txid}{index} → {txout}
  // persist block headers', block txids' and metadata rows:
  //      B{blockhash} → {header}
  //      X{blockhash} → {txid1}...{txidN}
  //      M{blockhash} → {tx_count}{size}{weight}
  block_entries
      .par_iter() // serialization is CPU-intensive
      .map(|b| {
          let mut rows = vec![];
          let blockhash = full_hash(&b.entry.hash()[..]);
          let txids: Vec<Txid> = b.block.txdata.iter().map(|tx| tx.txid()).collect();
          for tx in &b.block.txdata {
              add_transaction(tx, blockhash, &mut rows, iconfig);
          }

          if !iconfig.light_mode {
              rows.push(BlockRow::new_txids(blockhash, &txids).into_row());
              rows.push(BlockRow::new_meta(blockhash, &BlockMeta::from(b)).into_row());
          }

          rows.push(BlockRow::new_header(&b).into_row());
          rows.push(BlockRow::new_done(blockhash).into_row()); // mark block as "added"
          rows
      })
      .flatten()
      .collect()
}

fn add_transaction(
  tx: &Transaction,
  blockhash: FullHash,
  rows: &mut Vec<DBRow>,
  iconfig: &IndexerConfig,
) {
  rows.push(TxConfRow::new(tx, blockhash).into_row());

  if !iconfig.light_mode {
      rows.push(TxRow::new(tx).into_row());
  }

  let txid = full_hash(&tx.txid()[..]);
  for (txo_index, txo) in tx.output.iter().enumerate() {
      if is_spendable(txo) {
          rows.push(TxOutRow::new(&txid, txo_index, txo).into_row());
      }
  }
}

pub fn get_previous_txos(block_entries: &[BlockEntry]) -> BTreeSet<OutPoint> {
  block_entries
      .iter()
      .flat_map(|b| b.block.txdata.iter())
      .flat_map(|tx| {
          tx.input
              .iter()
              .filter(|txin| has_prevout(txin))
              .map(|txin| txin.previous_output)
      })
      .collect()
}

pub fn lookup_txos(
  txstore_db: &BaseCDBStore,
  outpoints: &BTreeSet<OutPoint>,
  allow_missing: bool,
) -> HashMap<OutPoint, TxOut> {
  let pool = rayon::ThreadPoolBuilder::new()
      .num_threads(16) // we need to saturate SSD IOPS
      .thread_name(|i| format!("lookup-txo-{}", i))
      .build()
      .unwrap();
  pool.install(|| {
      outpoints
          .par_iter()
          .filter_map(|outpoint| {
              lookup_txo(&txstore_db, &outpoint)
                  .or_else(|| {
                      if !allow_missing {
                          panic!("missing txo {} in {:?}", outpoint, txstore_db);
                      }
                      None
                  })
                  .map(|txo| (*outpoint, txo))
          })
          .collect()
  })
}

pub fn lookup_txo(txstore_db: &BaseCDBStore, outpoint: &OutPoint) -> Option<TxOut> {
  txstore_db
      .get(&TxOutRow::key(&outpoint))
      .map(|val| deserialize(&val).expect("failed to parse TxOut"))
}

pub fn index_blocks(
  block_entries: &[BlockEntry],
  previous_txos_map: &HashMap<OutPoint, TxOut>,
  iconfig: &IndexerConfig,
) -> Vec<Vec<DBRow>> {
  block_entries
      .par_iter() // serialization is CPU-intensive
      .map(|b| {
          let mut rows = vec![];
          let height = b.entry.height() as u32;
          for tx in &b.block.txdata {
              index_transaction(tx, height, previous_txos_map, &mut rows, iconfig);
          }
          rows.push(BlockRow::new_done(full_hash(&b.entry.hash()[..])).into_row()); // mark block as "indexed"
          rows
      })
      .collect()
}

// TODO: return an iterator?
pub fn index_transaction(
  tx: &Transaction,
  confirmed_height: u32,
  previous_txos_map: &HashMap<OutPoint, TxOut>,
  rows: &mut Vec<DBRow>,
  iconfig: &IndexerConfig,
) {
  // persist history index:
  //      H{funding-scripthash}{funding-height}F{funding-txid:vout} → ""
  //      H{funding-scripthash}{spending-height}S{spending-txid:vin}{funding-txid:vout} → ""
  // persist "edges" for fast is-this-TXO-spent check
  //      S{funding-txid:vout}{spending-txid:vin} → ""
  let txid = full_hash(&tx.txid()[..]);
  for (txo_index, txo) in tx.output.iter().enumerate() {
      if is_spendable(txo) || iconfig.index_unspendables {
          let history = TxHistoryRow::new(
              &txo.script_pubkey,
              confirmed_height,
              TxHistoryInfo::Funding(FundingInfo {
                  txid,
                  vout: txo_index as u16,
                  value: txo.value.amount_value(),
              }),
          );
          rows.push(history.into_row());

          if iconfig.address_search {
              if let Some(row) = addr_search_row(&txo.script_pubkey, iconfig.network) {
                  rows.push(row);
              }
          }
      }
  }
  for (txi_index, txi) in tx.input.iter().enumerate() {
      if !has_prevout(txi) {
          continue;
      }
      let prev_txo = previous_txos_map
          .get(&txi.previous_output)
          .unwrap_or_else(|| panic!("missing previous txo {}", txi.previous_output));

      let history = TxHistoryRow::new(
          &prev_txo.script_pubkey,
          confirmed_height,
          TxHistoryInfo::Spending(SpendingInfo {
              txid,
              vin: txi_index as u16,
              prev_txid: full_hash(&txi.previous_output.txid[..]),
              prev_vout: txi.previous_output.vout as u16,
              value: prev_txo.value.amount_value(),
          }),
      );
      rows.push(history.into_row());

      let edge = TxEdgeRow::new(
          full_hash(&txi.previous_output.txid[..]),
          txi.previous_output.vout as u16,
          txid,
          txi_index as u16,
      );
      rows.push(edge.into_row());
  }

  // Index issued assets & native asset pegins/pegouts/burns
  #[cfg(feature = "liquid")]
  asset::index_confirmed_tx_assets(
      tx,
      confirmed_height,
      iconfig.network,
      iconfig.parent_network,
      rows,
  );
}