use clap::Arg;
use clap::{command, crate_version};
use dirs::home_dir;
use std::fs;
use std::net::SocketAddr;
use std::net::ToSocketAddrs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use stderrlog;

use crate::daemon::cookie::CookieGetter;
use crate::chain::Network;
use txindex_errors::core::*;


const ELECTRS_VERSION: &str = env!("CARGO_PKG_VERSION");

fn get_or_default_str(m: &clap::ArgMatches, name: &str, default: &str) -> String {
    m.get_one::<String>(name)
    .map(|x|x.to_string())
    .unwrap_or(default.to_string())
}
#[derive(Debug, Clone)]
pub struct Config {
    // See below for the documentation of each field:
    pub log: stderrlog::StdErrLog,
    pub network_type: Network,
    pub db_path: PathBuf,
    pub daemon_dir: PathBuf,
    pub blocks_dir: PathBuf,
    pub daemon_rpc_addr: SocketAddr,
    pub cookie: Option<String>,
    pub http_addr: SocketAddr,
    pub http_socket_file: Option<PathBuf>,
    pub monitoring_addr: SocketAddr,
    pub jsonrpc_import: bool,
    pub light_mode: bool,
    pub address_search: bool,
    pub index_unspendables: bool,
    pub cors: Option<String>,
    pub precache_scripts: Option<String>,
    pub utxos_limit: usize,
    pub electrum_txs_limit: usize,
    pub electrum_banner: String,
    pub electrum_rpc_logging: Option<RpcLogging>,
}

fn str_to_socketaddr(address: &str, what: &str) -> SocketAddr {
    address
        .to_socket_addrs()
        .unwrap_or_else(|_| panic!("unable to resolve {} address", what))
        .collect::<Vec<_>>()
        .pop()
        .unwrap()
}

impl Config {
    pub fn from_args() -> Config {
        let network_help = format!("Select network type ({})", Network::names().join(", "));
        let rpc_logging_help = format!(
            "Select RPC logging option ({})",
            RpcLogging::options().join(", ")
        );

        let args = command!("txindex server")
            .version(crate_version!())
            .arg(
                Arg::new("verbosity")
                    .short('v')
                    .help("Increase logging verbosity"),
            )
            .arg(
                Arg::new("timestamp")
                    .long("timestamp")
                    .help("Prepend log lines with a timestamp"),
            )
            .arg(
                Arg::new("db_dir")
                    .long("db-dir")
                    .help("Directory to store index database (default: ./db/)"),
            )
            .arg(
                Arg::new("daemon_dir")
                    .long("daemon-dir")
                    .help("Data directory of Bitcoind (default: ~/.bitcoin/)"),
            )
            .arg(
                Arg::new("blocks_dir")
                    .long("blocks-dir")
                    .help("Analogous to bitcoind's -blocksdir option, this specifies the directory containing the raw blocks files (blk*.dat) (default: ~/.bitcoin/blocks/)"),
            )
            .arg(
                Arg::new("cookie")
                    .long("cookie")
                    .help("JSONRPC authentication cookie ('USER:PASSWORD', default: read from ~/.bitcoin/.cookie)")
                    ,
            )
            .arg(
                Arg::new("network")
                    .long("network")
                    .help(&network_help),
            )
            .arg(
                Arg::new("electrum_rpc_addr")
                    .long("electrum-rpc-addr")
                    .help("Electrum server JSONRPC 'addr:port' to listen on (default: '127.0.0.1:50001' for mainnet, '127.0.0.1:60001' for testnet and '127.0.0.1:60401' for regtest)"),
            )
            .arg(
                Arg::new("http_addr")
                    .long("http-addr")
                    .help("HTTP server 'addr:port' to listen on (default: '127.0.0.1:3000' for mainnet, '127.0.0.1:3001' for testnet and '127.0.0.1:3002' for regtest)"),
            )
            .arg(
                Arg::new("daemon_rpc_addr")
                    .long("daemon-rpc-addr")
                    .help("Bitcoin daemon JSONRPC 'addr:port' to connect (default: 127.0.0.1:8332 for mainnet, 127.0.0.1:18332 for testnet and 127.0.0.1:18443 for regtest)"),
            )
            .arg(
                Arg::new("monitoring_addr")
                    .long("monitoring-addr")
                    .help("Prometheus monitoring 'addr:port' to listen on (default: 127.0.0.1:4224 for mainnet, 127.0.0.1:14224 for testnet and 127.0.0.1:24224 for regtest)"),
            )
            .arg(
                Arg::new("jsonrpc_import")
                    .long("jsonrpc-import")
                    .help("Use JSONRPC instead of directly importing blk*.dat files. Useful for remote full node or low memory system"),
            )
            .arg(
                Arg::new("light_mode")
                    .long("lightmode")
                    .help("Enable light mode for reduced storage")
            )
            .arg(
                Arg::new("address_search")
                    .long("address-search")
                    .help("Enable prefix address search")
            )
            .arg(
                Arg::new("index_unspendables")
                    .long("index-unspendables")
                    .help("Enable indexing of provably unspendable outputs")
            )
            .arg(
                Arg::new("cors")
                    .long("cors")
                    .help("Origins allowed to make cross-site requests")
            )
            .arg(
                Arg::new("precache_scripts")
                    .long("precache-scripts")
                    .help("Path to file with list of scripts to pre-cache")
            )
            .arg(
                Arg::new("utxos_limit")
                    .long("utxos-limit")
                    .help("Maximum number of utxos to process per address. Lookups for addresses with more utxos will fail. Applies to the Electrum and HTTP APIs.")
                    .default_value("500")
            )
            .arg(
                Arg::new("electrum_txs_limit")
                    .long("electrum-txs-limit")
                    .help("Maximum number of transactions returned by Electrum history queries. Lookups with more results will fail.")
                    .default_value("500")
            ).arg(
                Arg::new("electrum_banner")
                    .long("electrum-banner")
                    .help("Welcome banner for the Electrum server, shown in the console to clients.")
            ).arg(
                Arg::new("electrum_rpc_logging")
                    .long("electrum-rpc-logging")
                    .help(&rpc_logging_help),
            );

        #[cfg(unix)]
        let args = args.arg(
                Arg::new("http_socket_file")
                    .long("http-socket-file")
                    .help("HTTP server 'unix socket file' to listen on (default disabled, enabling this disables the http server)"),
            );

        let m = args.get_matches();

        let network_name = get_or_default_str(&m, "network","mainnet");
        let network_type = Network::from(network_name.as_str());
        let t_db = get_or_default_str(&m, "db_dir", "./db");
        let db_dir = Path::new(&t_db);

        let db_path = db_dir.join(network_name);

        let default_daemon_port = match network_type {
            Network::Bitcoin => 8332,
            Network::Testnet => 18332,
            Network::Regtest => 18443,
            Network::Signet => 38332,
        };
        let default_http_port = match network_type {
            Network::Bitcoin => 3000,
            Network::Testnet => 3001,
            Network::Regtest => 3002,
            Network::Signet => 3003,
        };
        let default_monitoring_port = match network_type {
            Network::Bitcoin => 4224,
            Network::Testnet => 14224,
            Network::Regtest => 24224,
            Network::Signet => 54224,
        };

        let daemon_rpc_addr: SocketAddr = str_to_socketaddr(
            &get_or_default_str(&m, "daemon_rpc_addr",&format!("127.0.0.1:{}", default_daemon_port)),
            "Bitcoin RPC",
        );
        let http_addr: SocketAddr = str_to_socketaddr(
            &get_or_default_str(&m, "http_addr",&format!("127.0.0.1:{}", default_http_port)),
            "HTTP Server",
        );

        let http_socket_file: Option<PathBuf> = m.get_one::<String>("http_socket_file").map(PathBuf::from);
        let monitoring_addr: SocketAddr = str_to_socketaddr(
                &get_or_default_str(&m, "monitoring_addr",&format!("127.0.0.1:{}", default_monitoring_port)),

            "Prometheus monitoring",
        );

        let mut daemon_dir = m.get_one::<String>("daemon_dir")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                let mut default_dir = home_dir().expect("no homedir");
                default_dir.push(".bitcoin");
                default_dir
            });

        if let Some(network_subdir) = get_network_subdir(network_type) {
            daemon_dir.push(network_subdir);
        }
        let blocks_dir = m
            .get_one::<String>("blocks_dir")
            .map(PathBuf::from)
            .unwrap_or_else(|| daemon_dir.join("blocks"));
        let cookie = m.get_one::<String>("cookie").map(|s| s.to_owned());

        let electrum_banner = m.get_one::<String>("electrum_banner").map_or_else(
            || format!("Welcome to electrs-esplora {}", ELECTRS_VERSION),
            |s| s.into(),
        );

        let mut log = stderrlog::new();
        log.verbosity(m.get_count("verbosity") as usize);
        log.timestamp(if m.contains_id("timestamp") {
            stderrlog::Timestamp::Millisecond
        } else {
            stderrlog::Timestamp::Off
        });
        log.init().expect("logging initialization failed");
        let config = Config {
            log,
            network_type,
            db_path,
            daemon_dir,
            blocks_dir,
            daemon_rpc_addr,
            cookie,
            utxos_limit: 1000,
            electrum_txs_limit: 100,
            electrum_banner,
            electrum_rpc_logging: m
                .get_one::<String>("electrum_rpc_logging")
                .map(|option| RpcLogging::from(option.as_str())),
            http_addr,
            http_socket_file,
            monitoring_addr,
            jsonrpc_import: m.contains_id("jsonrpc_import"),
            light_mode: m.contains_id("light_mode"),
            address_search: m.contains_id("address_search"),
            index_unspendables: m.contains_id("index_unspendables"),
            cors: m.get_one::<String>("cors").map(|s| s.to_string()),
            precache_scripts: m.get_one::<String>("precache_scripts").map(|s| s.to_string()),

        };
        eprintln!("{:?}", config);
        config
    }

    pub fn cookie_getter(&self) -> Arc<dyn CookieGetter> {
        if let Some(ref value) = self.cookie {
            Arc::new(StaticCookie {
                value: value.as_bytes().to_vec(),
            })
        } else {
            Arc::new(CookieFile {
                daemon_dir: self.daemon_dir.clone(),
            })
        }
    }
}

#[derive(Debug, Clone)]
pub enum RpcLogging {
    Full,
    NoParams,
}

impl RpcLogging {
    pub fn options() -> Vec<String> {
        return vec!["full".to_string(), "no-params".to_string()];
    }
}

impl From<&str> for RpcLogging {
    fn from(option: &str) -> Self {
        match option {
            "full" => RpcLogging::Full,
            "no-params" => RpcLogging::NoParams,

            _ => panic!("unsupported RPC logging option: {:?}", option),
        }
    }
}

pub fn get_network_subdir(network: Network) -> Option<&'static str> {
    match network {
        Network::Bitcoin => None,
        Network::Testnet => Some("testnet3"),
        Network::Regtest => Some("regtest"),
        Network::Signet => Some("signet"),
    }
}

struct StaticCookie {
    value: Vec<u8>,
}

impl CookieGetter for StaticCookie {
    fn get(&self) -> Result<Vec<u8>> {
        Ok(self.value.clone())
    }
}

struct CookieFile {
    daemon_dir: PathBuf,
}

impl CookieGetter for CookieFile {
    fn get(&self) -> Result<Vec<u8>> {
        let path = self.daemon_dir.join(".cookie");
        let contents = fs::read(&path).chain_err(|| {
            ErrorKind::Connection(format!("failed to read cookie from {:?}", path))
        })?;
        Ok(contents)
    }
}
