use error_chain::error_chain;

error_chain! {
  types {
      Error, ErrorKind, ResultExt, Result;
  }

  errors {
      Connection(msg: String) {
          description("Connection error")
          display("Connection error: {}", msg)
      }

      Interrupt(sig: i32) {
          description("Interruption by external signal")
          display("Iterrupted by signal {}", sig)
      }

      TooPopular {
          description("Too many history entries")
          display("Too many history entries")
      }

      #[cfg(feature = "electrum-discovery")]
      ElectrumClient(e: electrum_client::Error) {
          description("Electrum client error")
          display("Electrum client error: {:?}", e)
      }

  }
}
