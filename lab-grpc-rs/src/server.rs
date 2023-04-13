//! The gRPC server.
//!

use crate::{log, rpc::kv_store::*, SERVER_ADDR};
use anyhow::Result;
use tonic::{transport::Server, Request, Response, Status};

use std::collections::HashMap;
use tokio::sync::RwLock;
use std::sync::Arc;

pub struct KvStore {
    kv_map_ptr: Arc<RwLock<HashMap<Vec<u8>,Vec<u8>>>>,
}

#[tonic::async_trait]
impl kv_store_server::KvStore for KvStore {
    async fn example(
        &self,
        req: Request<ExampleRequest>,
    ) -> Result<Response<ExampleReply>, Status> {
        log::info!("Received example request.");
        Ok(Response::new(ExampleReply {
            output: req.into_inner().input + 1,
        }))
    }

    async fn echo(
        &self,
        req: Request<EchoRequest>,
    ) -> Result<Response<EchoReply>, Status> {
        log::info!("Received echo request.");
        Ok(Response::new(EchoReply {
            msg: req.into_inner().msg,
        }))
    }

    /*Put should take in a key and value (both of type bytes) and store the pair in a hash map. 
    If the key is already in the hash map, the previous value should be overwritten.*/
    async fn put(
        &self,
        req: Request<PutRequest>,
    ) -> Result<Response<PutReply>, Status> {
        log::info!("Received put request.");
        let message = req.into_inner();
        let key:Vec<u8> = message.key;
        let value:Vec<u8> = message.value;
        /*check if key in map, if not, insert*/
        let kv_map = &mut self.kv_map_ptr.write().await;
        kv_map.entry(key).and_modify(|usize| *usize = value.clone()).or_insert(value);//need clone to perform deep copy of Vec

        Ok(Response::new(PutReply{}))
    }

    /*Get should take in a key of type bytes and return the result of looking up the value in the hash map. 
    If the key is not found, return Err(tonic::Status::new(tonic::Code::NotFound, "Key does not exist.")).*/
    async fn get(
        &self,
        req: Request<GetRequest>,
    ) -> Result<Response<GetReply>, Status> {
        log::info!("Received get request.");
        let key = req.into_inner().key;
        /*check if key in map*/
        let kv_map = &self.kv_map_ptr.read().await;
        match kv_map.get(&key){
            Some(val) => Ok(Response::new(GetReply{value: val.to_vec()})),
            None => Err(tonic::Status::new(tonic::Code::NotFound, "Key does not exist.")),
        }
    }
}

pub async fn start() -> Result<()> {
    let kv_map: HashMap<Vec<u8>,Vec<u8>> = HashMap::new();
    let kv_map_ptr = Arc::new(RwLock::new(kv_map));
    let svc = kv_store_server::KvStoreServer::new(KvStore {kv_map_ptr});

    log::info!("Starting KV store server.");
    Server::builder()
        .add_service(svc)
        .serve(SERVER_ADDR.parse().unwrap())
        .await?;
    Ok(())
}
