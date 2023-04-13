//! The gRPC client.
//!

use crate::{rpc::kv_store::*, SERVER_ADDR};
use anyhow::Result;
use tonic::transport::Channel;

async fn connect() -> Result<kv_store_client::KvStoreClient<Channel>> {
    Ok(kv_store_client::KvStoreClient::connect(format!("http://{}", SERVER_ADDR)).await?)
}

// Client methods. DO NOT MODIFY THEIR SIGNATURES.
/*server would return input+1*/
pub async fn example(input: u32) -> Result<u32> {
    let mut client = connect().await?;
    Ok(client
        .example(ExampleRequest { input })
        .await?
        .into_inner()
        .output)
}
pub async fn echo(msg: String) -> Result<String> {
    let mut client = connect().await?;
    Ok(client
        .echo(EchoRequest{msg})
        .await?
        .into_inner()
        .msg)
}
/*Put should take in a key and value (both of type bytes) and store the pair in a hash map. 
If the key is already in the hash map, the previous value should be overwritten.*/
pub async fn put(key: Vec<u8>, value: Vec<u8>) -> Result<()> {
    let mut client = connect().await?;
    client.put(PutRequest{key,value}).await?;
    Ok(())
}
 /*Get should take in a key of type bytes and return the result of looking up the value in the hash map. 
    If the key is not found, return Err(tonic::Status::new(tonic::Code::NotFound, "Key does not exist.")).*/
pub async fn get(key: Vec<u8>) -> Result<Vec<u8>> {
    let mut client = connect().await?;
    Ok(client
        .get(GetRequest{key})
        .await?
        .into_inner()
        .value)
}
