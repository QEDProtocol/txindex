use bitcoin::hashes::Hash;
pub use bitcoin::network::Network as BNetwork;
use serde::Serialize;

pub use bitcoin::{
    address, blockdata::block::Header as BlockHeader, blockdata::script, consensus::deserialize,
    hash_types::TxMerkleNode, Address, Block, BlockHash, OutPoint, ScriptBuf as Script, Sequence,
    Transaction, TxIn, TxOut, Txid,
};

#[derive(Debug, Copy, Clone, PartialEq, Hash, Serialize, Ord, PartialOrd, Eq)]
pub enum Network {
    Bitcoin,
    Testnet,
    Regtest,
    Signet,
}


impl From<&String> for Network {
    fn from(network_name: &String) -> Self {
        let nn = network_name.as_str();
        match nn {
            "mainnet" => Network::Bitcoin,
            "testnet" => Network::Testnet,
            "regtest" => Network::Regtest,
            "signet" => Network::Signet,
            _ => panic!("unsupported Bitcoin network: {:?}", network_name),
        }
    }
  }

impl From<&str> for Network {
  fn from(network_name: &str) -> Self {
      match network_name {
          "mainnet" => Network::Bitcoin,
          "testnet" => Network::Testnet,
          "regtest" => Network::Regtest,
          "signet" => Network::Signet,
          _ => panic!("unsupported Bitcoin network: {:?}", network_name),
      }
  }
}

impl From<Network> for BNetwork {
  fn from(network: Network) -> Self {
      match network {
          Network::Bitcoin => BNetwork::Bitcoin,
          Network::Testnet => BNetwork::Testnet,
          Network::Regtest => BNetwork::Regtest,
          Network::Signet => BNetwork::Signet,
      }
  }
}



#[cfg(not(feature = "liquid"))]
pub type Value = u64;


impl Network {
  pub fn magic(self) -> u32 {
      u32::from_le_bytes(BNetwork::from(self).magic().to_bytes())
  }

  pub fn is_regtest(self) -> bool {
      match self {
          Network::Regtest => true,
          _ => false,
      }
  }

  pub fn names() -> Vec<String> {
      return vec![
          "mainnet".to_string(),
          "testnet".to_string(),
          "regtest".to_string(),
          "signet".to_string(),
      ];
  }
}


pub fn genesis_hash(network: Network) -> BlockHash {
    return bitcoin_genesis_hash(network.into());
}

pub fn bitcoin_genesis_hash(network: BNetwork) -> bitcoin::BlockHash {
    match network {
        BNetwork::Bitcoin => BlockHash::from_byte_array(hex_literal::hex!(
            "9156352c1818b32e90c9e792efd6a11a82fe7956a630f03bbee236cedae3911a"
        )),
        BNetwork::Testnet => BlockHash::from_byte_array(hex_literal::hex!(
            "9e555073d0c4f36456db8951f449704d544d2826d9aa60636b40374626780abb"
        )),
        BNetwork::Regtest => BlockHash::from_byte_array(hex_literal::hex!(
            "a573e91c1772076c0d40f70e4408c83a31705f296ae6e7629d4adcb5a360213d"
        )),
        BNetwork::Signet => BlockHash::from_byte_array(hex_literal::hex!(
            "1a91e3dace36e2be3bf030a65679fe821aa1d6ef92e7c9902eb318182c355691"
        )),
        _ => panic!("unknown network {:?}", network),
    }
}