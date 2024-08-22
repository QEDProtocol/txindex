#!/bin/bash

CURRENT_DIR="$(pwd)"
RPC_URL="http://devnet:devnet@localhost:1337/bitcoin-rpc"
DB_PATH="$CURRENT_DIR/test-db"



sub_start_example() {
  cargo build --release
  echo "Starting example"
  echo "RPC_URL: $RPC_URL"
  echo "DB_PATH: $DB_PATH"
  rm -rf "$DB_PATH"
  mkdir -p "$DB_PATH"
  ./target/release/txi_example_server --jsonrpc-import --network regtest --cookie "devnet:devnet" --daemon-rpc-url "http://127.0.0.1:1337/bitcoin-rpc/" --db-dir "$DB_PATH" -vvvv
}

sub_start_example2() {
  cargo build --release
  echo "Starting example"
  echo "RPC_URL: $RPC_URL"
  echo "DB_PATH: $DB_PATH"
  rm -rf "$DB_PATH"
  mkdir -p "$DB_PATH"
  ./target/release/txi_example_server --jsonrpc-import --network regtest --cookie "devnet:devnet" --daemon-rpc-url "http://127.0.0.1:1442/bitcoin-rpc/" --db-dir "$DB_PATH" -vvvv
}


sub_help() {
  echo -e "Usage:\nStart an example server that jsonrpc-import's from http://devnet:devnet@localhost:1337/bitcoin-rpc\n./test.sh start_example\n"
}


subcommand=$1
case $subcommand in
    "" | "-h" | "--help")
        sub_help
        ;;
    *)
        shift
        sub_${subcommand} $@
        if [ $? = 127 ]; then
            echo "Error: '$subcommand' is not a known subcommand." >&2
            echo "       Run '$ProgName --help' for a list of known subcommands." >&2
            exit 1
        fi
        ;;
esac