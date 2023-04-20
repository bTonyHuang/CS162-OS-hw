//! The MapReduce coordinator.
//!

use anyhow::Result;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

use tokio::sync::Mutex;
use tokio::sync::RwLock;
use tokio::time::{Duration, Instant};
use std::sync::Arc;
use std::collections::VecDeque;
use std::collections::HashMap;

use crate::rpc::coordinator::*;
use crate::{log,*};

pub mod args;

pub struct Coordinator {
    //register count
    idcount_ptr: Arc<RwLock<u32>>,
    //hashmap for workers
    workermap_ptr: Arc<RwLock<HashMap<u32,Instant>>>,
}

#[tonic::async_trait]
impl coordinator_server::Coordinator for Coordinator {
    /// An example RPC.
    ///
    /// Feel free to delete this.
    /// Make sure to also delete the RPC in `proto/coordinator.proto`.
    async fn example(
        &self,
        req: Request<ExampleRequest>,
    ) -> Result<Response<ExampleReply>, Status> {
        let req = req.get_ref();
        let message = format!("Hello, {}!", req.name);
        Ok(Response::new(ExampleReply { message }))
    }

    async fn submit_job(
        &self,
        req: Request<SubmitJobRequest>,
    ) -> Result<Response<SubmitJobReply>, Status> {
        todo!("Job submission")
    }

    async fn poll_job(
        &self,
        req: Request<PollJobRequest>,
    ) -> Result<Response<PollJobReply>, Status> {
        todo!("Job submission")
    }

    //keep track of the time of the most recent heartbeat from each registered worker
    async fn heartbeat(
        &self,
        req: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatReply>, Status> {
        log::info!("Received heartbeat request.");
        let worker_id = req.into_inner().worker_id;
        let workermap = &mut self.workermap_ptr.write().await;
        let value = Instant::now();
        workermap.entry(worker_id).and_modify(|usize| *usize = value.clone()).or_insert(value);
        Ok(Response::new(HeartbeatReply {}))
    }

    // give a unique id(begin with 1), increasing 1 each time
    async fn register(
        &self,
        _req: Request<RegisterRequest>,
    ) -> Result<Response<RegisterReply>, Status> {
        log::info!("Received register request.");
        let id_count = &mut self.idcount_ptr.write().await;
        **id_count+=1;
        log::info!("Reply register request, id={}",**id_count);
        Ok(Response::new(RegisterReply { worker_id: **id_count }))
    }

    async fn get_task(
        &self,
        req: Request<GetTaskRequest>,
    ) -> Result<Response<GetTaskReply>, Status> {
        // TODO: Tasks
        Ok(Response::new(GetTaskReply {
            job_id: 0,
            output_dir: "".to_string(),
            app: "".to_string(),
            task: 0,
            file: "".to_string(),
            n_reduce: 0,
            n_map: 0,
            reduce: false,
            wait: true,
            map_task_assignments: Vec::new(),
            args: Vec::new(),
        }))
    }

    async fn finish_task(
        &self,
        req: Request<FinishTaskRequest>,
    ) -> Result<Response<FinishTaskReply>, Status> {
        // TODO: Tasks
        Ok(Response::new(FinishTaskReply {}))
    }

    async fn fail_task(
        &self,
        req: Request<FailTaskRequest>,
    ) -> Result<Response<FailTaskReply>, Status> {
        // TODO: Fault tolerance
        Ok(Response::new(FailTaskReply {}))
    }
}

pub async fn start(_args: args::Args) -> Result<()> {
    let addr = COORDINATOR_ADDR.parse().unwrap();
    let idcounter=0;
    let workermap:HashMap<u32,Instant>=HashMap::new();
    let idcount_ptr = Arc::new(RwLock::new(idcounter));

    let workermap_ptr = Arc::new(RwLock::new(workermap));
    let svc = coordinator_server::CoordinatorServer::new(Coordinator{idcount_ptr,workermap_ptr});
    Server::builder().add_service(svc).serve(addr).await?;

    Ok(())
}
