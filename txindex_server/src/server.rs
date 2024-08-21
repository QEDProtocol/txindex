use std::{process, sync::{Arc, RwLock}, time::Duration};

use log::{debug, info, warn};
use txindex_common::{config::Config, db::kvstore::{BaseCDBStore, BaseKVQStore, TxIndexStore}, utils::block::HeaderList, worker::traits::TxIndexWorker};
use bitcoin::consensus::encode::deserialize;

use crate::{api::traits::TxIndexRESTHandler, daemon::{daemon::Daemon, fetcher::FetchFrom, indexer::Indexer, mempool::Mempool, query::Query, schema::{load_blockhashes, load_blockheaders, BlockRow, ChainQuery}}, utils::{metrics::{MetricOpts, Metrics}, signal::Waiter}};
use crate::api::rest;
use txindex_errors::core::*;
use error_chain::ChainedError;

fn fetch_from(config: &Config, store: &TxIndexStore) -> FetchFrom {
  let mut jsonrpc_import = config.jsonrpc_import;
  if !jsonrpc_import {
      // switch over to jsonrpc after the initial sync is done
      jsonrpc_import = store.done_initial_sync();
  }

  if jsonrpc_import {
      // slower, uses JSONRPC (good for incremental updates)
      FetchFrom::Bitcoind
  } else {
      // faster, uses blk*.dat files (good for initial indexing)
      FetchFrom::BlkFiles
  }
}
pub fn open_tx_index_store(config: Arc<Config>) -> TxIndexStore {
  let path = config.db_path.join("newindex");

  let txstore_db = BaseCDBStore::open(&path.join("txstore"), config.light_mode);
  let added_blockhashes = load_blockhashes(&txstore_db, &BlockRow::done_filter());
  debug!("{} blocks were added", added_blockhashes.len());

  let history_db = BaseCDBStore::open(&path.join("history"), config.light_mode);
  let indexed_blockhashes = load_blockhashes(&history_db, &BlockRow::done_filter());
  debug!("{} blocks were indexed", indexed_blockhashes.len());

  let cache_db = BaseCDBStore::open(&path.join("cache"), config.light_mode);

  let headers = if let Some(tip_hash) = txstore_db.get(b"t") {
      let tip_hash = deserialize(&tip_hash).expect("invalid chain tip in `t`");
      let headers_map = load_blockheaders(&txstore_db);
      debug!(
          "{} headers were loaded, tip at {:?}",
          headers_map.len(),
          tip_hash
      );
      HeaderList::new(headers_map, tip_hash)
  } else {
      HeaderList::empty()
  };

  let indexer_db =BaseKVQStore::open_default(path.join("indexer_db"));
  if indexer_db.is_err() {
    panic!("failed to open indexer_db");
  }
  let indexer_db =  Arc::new(indexer_db.unwrap());
  TxIndexStore {
    txstore_db,
    history_db,
    cache_db,
    indexer_db,
    added_blockhashes: RwLock::new(added_blockhashes),
    indexed_blockhashes: RwLock::new(indexed_blockhashes),
    indexed_headers: RwLock::new(headers),
}

}
pub fn start_txindex_server_with_config<API: 'static + TxIndexRESTHandler + Clone + Send + Sync, I: TxIndexWorker<BaseKVQStore, ChainQuery>>(config: Arc<Config>) -> Result<()> {
  let signal = Waiter::start();
  let metrics = Metrics::new(config.monitoring_addr);
  metrics.start();

  let daemon = Arc::new(Daemon::new(
      &config.daemon_dir,
      &config.blocks_dir,
      config.daemon_rpc_addr,
      config.cookie_getter(),
      config.network_type,
      signal.clone(),
      &metrics,
  )?);
  let store = Arc::new(open_tx_index_store(config.clone()));
  let mut indexer = Indexer::open(
      Arc::clone(&store),
      fetch_from(&Arc::clone(&config), &store),
      &config,
      &metrics,
  );

  let chain = Arc::new(ChainQuery::new(
      Arc::clone(&store),
      Arc::clone(&daemon),
      &config,
      &metrics,
  ));

  let mut tip = indexer.update::<I, ChainQuery>(&daemon, Arc::clone(&chain))?;

  let mempool = Arc::new(RwLock::new(Mempool::new(
      Arc::clone(&chain),
      &metrics,
      Arc::clone(&config),
  )));
  loop {
      match Mempool::update(&mempool, &daemon) {
          Ok(_) => break,
          Err(e) => {
              warn!("Error performing initial mempool update, trying again in 5 seconds: {}", e.display_chain());
              signal.wait(Duration::from_secs(5), false)?;
          },
      }
  }

  let query = Arc::new(Query::new(
      Arc::clone(&chain),
      Arc::clone(&mempool),
      Arc::clone(&daemon),
      Arc::clone(&config),
  ));

  // TODO: configuration for which servers to start
  let rest_server = rest::start::<API>(Arc::clone(&config), Arc::clone(&query));

  let main_loop_count = metrics.gauge(MetricOpts::new(
      "electrs_main_loop_count",
      "count of iterations of electrs main loop each 5 seconds or after interrupts",
  ));

  loop {

      main_loop_count.inc();

      if let Err(err) = signal.wait(Duration::from_secs(5), true) {
          info!("stopping server: {}", err);
          rest_server.stop();
          // the electrum server is stopped when dropped
          break;
      }

      // Index new blocks
      let current_tip = daemon.getbestblockhash()?;
      if current_tip != tip {
          indexer.update::<I, ChainQuery>(&daemon, Arc::clone(&chain))?;
          tip = current_tip;
      };

      // Update mempool
      if let Err(e) = Mempool::update(&mempool, &daemon) {
          // Log the error if the result is an Err
          warn!("Error updating mempool, skipping mempool update: {}", e.display_chain());
      }

      // Update subscribed clients
      //electrum_server.notify();
  }
  info!("server stopped");
  Ok(())
}

pub fn start_txindex_server<API: 'static + TxIndexRESTHandler + Clone + Send + Sync, I: TxIndexWorker<BaseKVQStore, ChainQuery>>() {
  let config = Arc::new(Config::from_args());
  if let Err(e) = start_txindex_server_with_config::<API, I>(config) {
      log::error!("server failed: {}", e.display_chain());
      process::exit(1);
  }
}
