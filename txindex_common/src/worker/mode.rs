#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxWorkerProcessingMode {
  Block,
  BlockWithBlockNumberSuffix,
  Transactions,
  TransactionsWithBlockNumberSuffix,
}


