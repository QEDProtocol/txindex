use api::ExampleRESTHandler;
use txindex_server::server::start_txindex_server;
use workers::ExampleRootWorker;

pub mod tables;
pub mod workers;
pub mod api;

fn main() {
    start_txindex_server::<ExampleRESTHandler, ExampleRootWorker>();
}