
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
impl From<String> for RpcLogging {
    fn from(option: String) -> Self {
        RpcLogging::from(option.as_str())
    }
}
impl From<&String> for RpcLogging {
    fn from(option: &String) -> Self {
        RpcLogging::from(option.as_str())
    }
}