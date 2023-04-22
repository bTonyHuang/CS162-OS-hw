//! The MapReduce coordinator.
//!

use anyhow::Result;
use tonic::transport::Server;
use tonic::{Request, Response, Status};
use tonic::Code;

use tokio::sync::Mutex;
use tokio::time::{Duration, Instant};
use std::sync::Arc;
use std::collections::VecDeque;
use std::collections::HashMap;

use crate::rpc::coordinator::*;
use crate::{log,*};

use crate::app::named;

pub mod args;

pub struct JobInfo {
    job_id: JobId,
    files: Vec<String>,  //input files, repeated strings
    output_dir: String,
    app: String,    //application
    n_reduce: u32,
    args: Vec<u8>,  //args bytes
    done: bool,
    failed: bool,
    errors: Vec<String>, //error information from worker application
}

impl JobInfo {
    pub fn new(
        job_id: JobId,
        files: Vec<String>, 
        output_dir: String,
        app: String,    
        n_reduce: u32,
        args: Vec<u8>,  
        done: bool,
        failed: bool,
        errors: Vec<String>,
    ) -> Self{
        JobInfo {
            job_id,
            files,
            output_dir,
            app,
            n_reduce,
            args,
            done,
            failed,
            errors,
        }
    }//end of new
}

//mutable state
pub struct CoordinatorState {
    //worker register count
    workerid_count: WorkerId,
    //hashmap for workers heartbeat
    workerheartbeat_map: HashMap<WorkerId,Instant>,
    //job register count
    jobid_count: JobId,
    //job queue
    job_queue: VecDeque<JobId>,
    //hashmap for job information
    jobinfo_map: HashMap<JobId, JobInfo>,
}

impl CoordinatorState {
    pub fn new(
        //worker register count
        workerid_count: WorkerId,
        //hashmap for workers heartbeat
        workerheartbeat_map: HashMap<WorkerId,Instant>,
        //job register count
        jobid_count: JobId,
        //job queue
        job_queue: VecDeque<JobId>,
        //hashmap for job information
        jobinfo_map: HashMap<JobId, JobInfo>,
    ) -> Self {
        CoordinatorState {
            workerid_count,
            workerheartbeat_map,
            jobid_count,
            job_queue,
            jobinfo_map,
        }
    }//end of new
}

//immutable state
pub struct Coordinator {
    //syncronization
    inner: Arc<Mutex<CoordinatorState>>,
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
        let message = req.into_inner();
        //check if the provided application name is valid using the crate::app::named function. 
        if let Err(e) = named(&message.app) {
            return Err(Status::new(Code::NotFound, e.to_string()));
        }
        let state = &mut self.inner.lock().await;
        state.jobid_count += 1; //job id start with 1, increasing 1 each time
        let jobid = state.jobid_count;

        let done = false;
        let failed = false;
        let errors:Vec<String> = Vec::new();
        
        let jobinfo = JobInfo::new(
            jobid,
            message.files,
            message.output_dir,
            message.app,
            message.n_reduce,
            message.args,
            done,
            failed,
            errors,
        );
        state.job_queue.push_back(jobid);
        state.jobinfo_map.insert(jobid, jobinfo);

        Ok(Response::new(SubmitJobReply {job_id: jobid}))
    }

    async fn poll_job(
        &self,
        req: Request<PollJobRequest>,
    ) -> Result<Response<PollJobReply>, Status> {
        let jobid = req.into_inner().job_id;
        let state = &mut self.inner.lock().await;

        match state.jobinfo_map.get(&jobid) {
            Some(jobinfo) => Ok(Response::new(PollJobReply {done:jobinfo.done, failed:jobinfo.failed, errors: jobinfo.errors.clone()})),
            None => Err(Status::new(Code::NotFound, "job id is invalid")),
        }
    }

    //keep track of the time of the most recent heartbeat from each registered worker
    async fn heartbeat(
        &self,
        req: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatReply>, Status> {
        log::info!("Received heartbeat request.");
        let worker_id = req.into_inner().worker_id;
        let state = &mut self.inner.lock().await;
        let value = Instant::now();
        state.workerheartbeat_map.entry(worker_id).and_modify(|usize| *usize = value.clone()).or_insert(value);
        Ok(Response::new(HeartbeatReply {}))
    }

    // give a unique id(begin with 1), increasing 1 each time
    async fn register(
        &self,
        _req: Request<RegisterRequest>,
    ) -> Result<Response<RegisterReply>, Status> {
        log::info!("Received register request.");
        let state = &mut self.inner.lock().await;
        state.workerid_count += 1;
        log::info!("Reply register request, id={}", state.workerid_count);
        Ok(Response::new(RegisterReply { worker_id: state.workerid_count}))
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
    //initialize coordinator
    let addr = COORDINATOR_ADDR.parse().unwrap();
    let workerid_count:WorkerId = 0;
    let workerheartbeat_map:HashMap<WorkerId,Instant> = HashMap::new();
    let jobid_count:JobId = 0;
    let job_queue:VecDeque<JobId> = VecDeque::new();
    let jobinfo_map:HashMap<JobId,JobInfo> = HashMap::new();
    let mut_state: CoordinatorState = CoordinatorState::new(workerid_count,workerheartbeat_map,jobid_count,job_queue,jobinfo_map);

    //syncronization
    let inner: Arc<Mutex<CoordinatorState>> = Arc::new(Mutex::new(mut_state));

    let svc = coordinator_server::CoordinatorServer::new(Coordinator{inner});
    Server::builder().add_service(svc).serve(addr).await?;

    Ok(())
}
