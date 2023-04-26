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
use crate::task::JobInfo;
use crate::task::TaskInfo;
use crate::task::WorkerInfo;

pub mod args;

//mutable state
pub struct CoordinatorState {
    //worker register count
    workerid_count: WorkerId,
    //hashmap for workers information
    workerinfo_map: HashMap<WorkerId,WorkerInfo>,
    //job register count, priority since we use FIFO
    jobid_count: JobId,
    //hashmap for job information
    jobinfo_map: HashMap<JobId, JobInfo>,
    //task queue
    task_queue: VecDeque<TaskInfo>,
}

impl CoordinatorState {
    pub fn new(
        //worker register count
        workerid_count: WorkerId,
        //hashmap for workers information
        workerinfo_map: HashMap<WorkerId,WorkerInfo>,
        //job register count
        jobid_count: JobId,
        //hashmap for job information
        jobinfo_map: HashMap<JobId, JobInfo>,
        //task queue
        task_queue: VecDeque<TaskInfo>
    ) -> Self {
        CoordinatorState {
            workerid_count,
            workerinfo_map,
            jobid_count,
            jobinfo_map,
            task_queue
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
        log::info!("Received submit_job request.");
        let message = req.into_inner();
        //check if the provided application name is valid using the crate::app::named function. 
        if let Err(e) = named(&message.app) {
            return Err(Status::new(Code::NotFound, e.to_string()));
        }
        log::info!("Pass app name check.");
        let state = &mut self.inner.lock().await;
        log::info!("create job.");
        state.jobid_count += 1; //job id start with 1, increasing 1 each time
        let jobid = state.jobid_count;
        let files = message.files.clone();

        //jobinfo fields
        let done = false;
        let failed = false;
        let errors:Vec<String> = Vec::new();
        let mut task_map:HashMap<TaskNumber,TaskInfo> = HashMap::new();

        //taskinfo fields
        let output_dir = message.output_dir.clone();
        let app = message.app.clone();
        let n_map = files.len() as u32;
        let n_reduce = message.n_reduce;
        let reduce = false;
        let wait = false;
        let map_task_assignments = Vec::new();
        let args = message.args.clone();
        log::info!("n_map = {}",n_map);
        //initialize taskinfo and inqueue
        for i in 0..n_map as TaskNumber{
            let taskinfo = TaskInfo::new(
                jobid,
                output_dir.clone(),
                app.clone(),
                i as TaskNumber,//task number, begin from 0
                message.files[i].clone(),//file
                n_reduce,
                n_map,
                reduce,
                wait,
                map_task_assignments.clone(),
                args.clone(),
                0, //no worker
            );
            log::info!("insert task to task_map and task_queue");
            //store a copy in task_map for fault tolerance
            task_map.insert(i,taskinfo.clone());
            //push to queue
            state.task_queue.push_back(taskinfo);
        }

        let jobinfo = JobInfo::new(
            jobid,
            files,
            output_dir.clone(),
            app.clone(),
            n_reduce,
            args.clone(),
            done,
            failed,
            errors,
            task_map,
        );
        log::info!("insert job to jobinfo_map");
        state.jobinfo_map.insert(jobid, jobinfo);

        Ok(Response::new(SubmitJobReply {job_id: jobid}))
    }

    async fn poll_job(
        &self,
        req: Request<PollJobRequest>,
    ) -> Result<Response<PollJobReply>, Status> {
        log::info!("Received poll_job request.");
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
        state.workerinfo_map.entry(worker_id)
            .and_modify(|e| (*e).heartbeat = value.clone())
            .or_insert(WorkerInfo::new(0,0,value));
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
        log::info!("Received get_task request.");
        let state = &mut self.inner.lock().await;
        /*check heartbeat of each worker and put task on queue*/

        //assign task
        if let Some(taskinfo) = state.task_queue.pop_front() {
            log::info!("give task.");
            state.workerinfo_map.entry(req.into_inner().worker_id)
                .and_modify(|e|(*e).job_id = taskinfo.job_id)
                .and_modify(|e|(*e).task = taskinfo.task);
            return Ok(Response::new(GetTaskReply {
            job_id: taskinfo.job_id,
            output_dir: taskinfo.output_dir,
            app: taskinfo.app,
            task: taskinfo.task as u32,
            file: taskinfo.file,
            n_reduce: taskinfo.n_reduce,
            n_map: taskinfo.n_map,
            reduce: taskinfo.reduce,
            wait: false,
            map_task_assignments: taskinfo.map_task_assignments,
            args: taskinfo.args,
            }))
        }
        //keep worker idle
        else {
            log::info!("stay idle.");
            state.workerinfo_map.entry(req.into_inner().worker_id)
                .and_modify(|e|(*e).job_id = 0)
                .and_modify(|e|(*e).task = 0);
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
    }

    async fn finish_task(
        &self,
        req: Request<FinishTaskRequest>,
    ) -> Result<Response<FinishTaskReply>, Status> {
        log::info!("Received finish_task request.");
        let state = &mut self.inner.lock().await;
        let message = req.into_inner();
        let jobid = message.job_id;
        let workerid = message.worker_id;
        let task = message.task as TaskNumber;
        let mut reduce = message.reduce;

        //ASSERT(!workerid, false); //workerid should not be 0
        (*state.jobinfo_map.get_mut(&jobid).unwrap().task_map.get_mut(&task).unwrap())
            .worker_id=workerid;
        let mut taskinfo = state.jobinfo_map[&jobid].task_map[&task].clone();

        let n_reduce = taskinfo.n_reduce as TaskNumber;
        let n_map = taskinfo.n_map as TaskNumber;

        //check if all map task finish
        if !reduce {
            log::info!("finish map tasks");
            reduce = true;
            for i in 0.. n_map {
                if state.jobinfo_map[&jobid].task_map[&i].worker_id==0 {
                    reduce = false;
                    break;
                }
            }
            //finish all map tasks
            if reduce {
                log::info!("finish all map tasks");
                for i in 0..n_reduce {
                    taskinfo.reduce = true;
                    taskinfo.task = i;
                    //taskinfo.map_task_assignments
                    for i in 0.. n_map {
                        let task = i as u32;
                        let worker_id = state.jobinfo_map[&jobid].task_map[&i].worker_id;
                        let map_task_assignment = Response::new(MapTaskAssignment{task:task,worker_id:worker_id});
                        taskinfo.map_task_assignments.push(map_task_assignment.into_inner());
                    }
                    log::info!("assign reduce tasks");
                    state.jobinfo_map.get_mut(&jobid).unwrap().task_map.insert(n_map+i,taskinfo.clone());
                    state.task_queue.push_back(taskinfo.clone());
                }
            }
        }
        else {
            log::info!("finish reduce tasks");
            state.jobinfo_map.get_mut(&jobid).unwrap().done = true;
            for i in 0..n_reduce {
                if state.jobinfo_map[&jobid].task_map[&(n_map+i)].worker_id==0 {
                    state.jobinfo_map.get_mut(&jobid).unwrap().done = false;
                    break;
                }
            }
        }

        Ok(Response::new(FinishTaskReply {}))
    }

    async fn fail_task(
        &self,
        req: Request<FailTaskRequest>,
    ) -> Result<Response<FailTaskReply>, Status> {
        log::info!("Received fail_task request.");
        Ok(Response::new(FailTaskReply {}))
    }
}

pub async fn start(_args: args::Args) -> Result<()> {
    //initialize coordinator
    let addr = COORDINATOR_ADDR.parse().unwrap();
    let workerid_count:WorkerId = 0;
    let workerheartbeat_map:HashMap<WorkerId,WorkerInfo> = HashMap::new();
    let jobid_count:JobId = 0;
    let jobinfo_map:HashMap<JobId,JobInfo> = HashMap::new();
    let task_queue:VecDeque<TaskInfo> = VecDeque::new();
    let mut_state: CoordinatorState = CoordinatorState::new(
        workerid_count,
        workerheartbeat_map,
        jobid_count,
        jobinfo_map,
        task_queue,
    );

    //syncronization
    let inner: Arc<Mutex<CoordinatorState>> = Arc::new(Mutex::new(mut_state));

    let svc = coordinator_server::CoordinatorServer::new(Coordinator{inner});
    Server::builder().add_service(svc).serve(addr).await?;

    Ok(())
}
