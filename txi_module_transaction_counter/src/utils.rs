use bitcoin::{hashes::Hash, Address, ScriptBuf};

pub fn get_scriptpubkey_hash(script: &ScriptBuf) -> [u8; 32] {
    bitcoin::hashes::sha256::Hash::hash(script.as_bytes())
        .as_byte_array()
        .to_owned()
}

pub fn get_scriptpubkey_hash_for_address(address: &Address) -> [u8; 32] {
    get_scriptpubkey_hash(&address.script_pubkey())
}
