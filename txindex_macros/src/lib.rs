

#[macro_export]
macro_rules! impl_kvq_serialize {
    ($($typ:ty),+ $(,)?) => {
        $(
            impl KVQSerializable for $typ {
                fn to_bytes(&self) -> anyhow::Result<Vec<u8>> {
                    Ok(self.to_be_bytes().to_vec())
                }
                fn from_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
                    Ok(<$typ>::from_be_bytes(bytes.try_into()?))
                }
            }
        )+
    };
}
