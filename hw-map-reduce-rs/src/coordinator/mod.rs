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
    //job queue
    job_queue: VecDeque<JobId>,
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
        job_queue: VecDeque<JobId>
    ) -> Self {
        CoordinatorState {
            workerid_count,
            workerinfo_map,
            jobid_count,
            jobinfo_map,
            job_queue
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

        //jobinfo fields
        log::info!("create job.");
        state.jobid_count += 1; //job id start with 1, increasing 1 each time
        let jobid = state.jobid_count;
        let output_dir = message.output_dir;
        let app = message.app;
        let files = message.files.clone();
        let n_map = files.len() as u32;
        let n_reduce = message.n_reduce;
        let done = false;
        let failed = false;
        let errors:Vec<String> = Vec::new();
        let args = message.args.clone();
        let mut task_map:HashMap<TaskNumber,TaskInfo> = HashMap::new();
        let mut task_queue:VecDeque<TaskNumber> = VecDeque::new();
        let map_task_assignments:Vec<MapTaskAssignment> = Vec::new();
        let map_complete = 0;
        let reduce_complete = 0;

        //taskinfo fields
        let reduce = false;
        let wait = false;

        log::info!("n_map = {}",n_map);
        //initialize taskinfo and inqueue
        for i in 0..n_map as TaskNumber{
            let taskinfo = TaskInfo::new(
                jobid,
                i,//task number, begin from 0
                message.files[i].clone(),//file
                reduce,
                wait,
                0, //no worker
            );
            log::info!("insert map task to task_map and task_queue");
            task_map.insert(i,taskinfo);
            task_queue.push_back(i);
        }

        let jobinfo = JobInfo::new(
            jobid,
            files,
            output_dir,
            app,
            n_reduce,
            args,
            done,
            failed,
            errors,
            task_map,
            task_queue,
            map_complete,
            reduce_complete,
            map_task_assignments,
            n_map,
        );
        log::info!("insert job to jobinfo_map");
        state.jobinfo_map.insert(jobid, jobinfo);
        log::info!("insert job to job_queue");
        state.job_queue.push_back(jobid);

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
        /*initialize rpc items*/
        let mut job_id = 0;
        let mut output_dir = "".to_string();
        let mut app = "".to_string();
        let mut task = 0;
        let mut file = "".to_string();
        let mut n_reduce = 0;
        let mut n_map = 0;
        let mut reduce = false;
        let mut wait = true;
        let mut map_task_assignments = Vec::new();
        let mut args = Vec::new();

        let state = &mut self.inner.lock().await;
        //keep idle
        let workerid = req.into_inner().worker_id;
        state.workerinfo_map.entry(workerid)
            .and_modify(|e|(*e).job_id = 0)
            .and_modify(|e|(*e).task = 0);
        /*check heartbeat of each worker and put task on queue*/
        let task_timeout_secs = Duration::from_secs(TASK_TIMEOUT_SECS);
        let workerinfo_map = state.workerinfo_map.clone();
        for (workerid, workerinfo) in workerinfo_map.iter() {
            //worker timeout
            if workerinfo.heartbeat.elapsed() > task_timeout_secs {
                log::info!("worker {} timeout!", workerid);
                if workerinfo.job_id != 0 {//reassign job
                    log::info!("reassign job{}'s task {}",workerinfo.job_id, workerinfo.task);
                    let jobinfo = state.jobinfo_map.get_mut(&workerinfo.job_id).unwrap();
                    if workerinfo.task >= jobinfo.n_map as TaskNumber {
                        jobinfo.task_queue.push_back(workerinfo.task);
                    }
                    else{
                        jobinfo.task_queue.push_front(workerinfo.task);
                    }
                }
                state.workerinfo_map.remove(&workerid);
            }
        }

        //assign task
        let mut i = 0;
        //clone the job queue to avoid incorrect borrow
        let job_queue = state.job_queue.clone();
        loop{
            //iterate job queue
            if let Some(jobid) = job_queue.get(i) {
                let jobinfo = state.jobinfo_map.get_mut(jobid).unwrap();
                //get task from task queue
                if let Some(tasknumber) = jobinfo.task_queue.pop_front(){
                    if jobinfo.map_complete < jobinfo.n_map && tasknumber >= jobinfo.n_map as TaskNumber {
                        log::info!("map tasks are not all finished, stop assigning reduce task");
                        i+=1;
                        continue;
                    }
                    log::info!("give task.");
                    let taskinfo = jobinfo.task_map.get(&tasknumber).unwrap();
                    //update items
                    job_id = taskinfo.job_id;
                    output_dir = jobinfo.output_dir.clone();
                    app = jobinfo.app.clone();
                    task = taskinfo.task as u32;
                    file = taskinfo.file.clone();
                    n_reduce = jobinfo.n_reduce;
                    n_map = jobinfo.n_map;
                    reduce = taskinfo.reduce;
                    wait = taskinfo.wait;
                    map_task_assignments = jobinfo.map_task_assignments.clone();
                    args = jobinfo.args.clone();
                    //update workerinfo map
                    state.workerinfo_map.entry(workerid)
                        .and_modify(|e|(*e).job_id = job_id)
                        .and_modify(|e|(*e).task = tasknumber);
                    log::info!("job {}'s task {} given", job_id, tasknumber);
                    break;
                }
            }else{//no job avaliable
                log::info!("no jobs avaliable, job queue's length={}",i);
                break;
            }
            i += 1;
        }

        Ok(Response::new(GetTaskReply {
            job_id: job_id,
            output_dir: output_dir,
            app: app,
            task: task,
            file: file,
            n_reduce: n_reduce,
            n_map: n_map,
            reduce: reduce,
            wait: wait,
            map_task_assignments: map_task_assignments,
            args: args,
        }))
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
        let mut task = message.task as TaskNumber;
        let reduce = message.reduce;

        //get corresponding jobinfo and taskinfo, update status
        let jobinfo = state.jobinfo_map.get_mut(&jobid).unwrap();
        if reduce {
            task += jobinfo.n_map as usize;
        }
        let taskinfo = jobinfo.task_map.get_mut(&task).unwrap();
        taskinfo.worker_id = workerid;
        //map task finish
        if !reduce {
            log::info!("job {}'s map task {} finished", jobid, task);
            jobinfo.map_complete+=1;
            //check if map tasks all finish
            if jobinfo.map_complete == jobinfo.n_map {
                log::info!("job {}'s map tasks all finished", jobid);
                //create map_task_assignments
                jobinfo.map_task_assignments = Vec::new();
                for i in 0..jobinfo.n_map as TaskNumber {
                    let task = i as u32;
                    let worker_id = jobinfo.task_map[&i].worker_id;
                    let map_task_assignment = Response::new(MapTaskAssignment{task:task,worker_id:worker_id});
                    jobinfo.map_task_assignments.push(map_task_assignment.into_inner());
                }
                log::info!("job {}'s map_task_assignments created", jobid);

                //create reduce tasks
                for i in 0..jobinfo.n_reduce {
                    let tasknumber = (i+jobinfo.n_map) as TaskNumber;
                    if jobinfo.task_map.contains_key(&tasknumber) && jobinfo.task_map[&tasknumber].worker_id != 0 {
                        log::info!("job {}'s reduce task {} already finished, continue", jobid, tasknumber);
                        continue;
                    }
                    let reduce_taskinfo = TaskInfo::new(
                        jobid,
                        i as TaskNumber,//task number, begin from 0
                        "".to_string(),//file
                        true, //reduce
                        false, //wait
                        0, //no worker
                    );
                    log::info!("insert reduce task to task_map and task_queue");
                    jobinfo.task_map.insert((i+jobinfo.n_map) as TaskNumber,reduce_taskinfo);
                    jobinfo.task_queue.push_back((i+jobinfo.n_map) as TaskNumber);
                }
            }
        }
        //reduce task finish
        else {
            log::info!("job {}'s reduce task {} finished", jobid, task);
            jobinfo.reduce_complete+=1;
            //check if reduce tasks all finish
            if jobinfo.reduce_complete >= jobinfo.n_reduce {
                let success = true;
                for i in 0..(jobinfo.n_map + jobinfo.n_reduce) as TaskNumber {
                    if jobinfo.task_map[&i].worker_id == 0 {
                        success = false;
                        break;
                    }
                }
                if success {
                    log::info!("job {}'s reduce tasks all finished", jobid);
                    jobinfo.done = true;
                    for i in 0..state.job_queue.len(){
                        if *(state.job_queue.get(i).unwrap())==jobid {
                            state.job_queue.remove(i);//remove it from job_queue
                            break;
                        }
                    }
                }
            }
            //reduce task finished, reset worker to idle
            state.workerinfo_map.entry(workerid)
                .and_modify(|e|(*e).job_id = 0)
                .and_modify(|e|(*e).task = 0);
        }

        Ok(Response::new(FinishTaskReply {}))
    }

    async fn fail_task(
        &self,
        req: Request<FailTaskRequest>,
    ) -> Result<Response<FailTaskReply>, Status> {
        log::info!("Received fail_task request.");
        let message = req.into_inner();
        let workerid = message.worker_id;
        let jobid = message.job_id;
        let mut tasknumber = message.task;

        let state = &mut self.inner.lock().await;
        let workerinfo_map = state.workerinfo_map.clone();
        let jobinfo = state.jobinfo_map.get_mut(&jobid).unwrap();
        /*check retry status*/
        if message.retry {
            //check the task type
            if message.reduce {
                //reassign the reduce task
                tasknumber += jobinfo.n_map;
                jobinfo.task_queue.push_back(tasknumber as TaskNumber);
                //reassign map tasks 
                if jobinfo.map_complete == jobinfo.n_map {
                    //check map task info to find failed worker
                    for i in 0..jobinfo.n_map as TaskNumber{
                        let workerid = jobinfo.task_map[&i].worker_id;
                        if !workerinfo_map.contains_key(&workerid) {
                            log::info!("reassign map task {}",i);
                            jobinfo.task_queue.push_front(i);
                            jobinfo.map_complete-=1; //stop assigning reduce tasks
                        }
                    }   
                }
            }
            else{
                //simply reassign the map task
                log::info!("reassign map task {}", tasknumber);
                jobinfo.task_queue.push_front(tasknumber as TaskNumber);
            }
        }
        else {
            //job failures
            log::info!{"job {} failed",jobid};
            jobinfo.failed = true;
            jobinfo.errors.push(message.error.clone());
            for i in 0..state.job_queue.len(){
                if *(state.job_queue.get(i).unwrap())== jobid {
                    state.job_queue.remove(i);//remove it from job_queue
                    break;
                }
            }
        }
        //reset worker to idle
        state.workerinfo_map.entry(workerid)
            .and_modify(|e|(*e).job_id = 0)
            .and_modify(|e|(*e).task = 0);

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
    let job_queue:VecDeque<JobId> = VecDeque::new();
    let mut_state: CoordinatorState = CoordinatorState::new(
        workerid_count,
        workerheartbeat_map,
        jobid_count,
        jobinfo_map,
        job_queue,
    );

    //syncronization
    let inner: Arc<Mutex<CoordinatorState>> = Arc::new(Mutex::new(mut_state));

    let svc = coordinator_server::CoordinatorServer::new(Coordinator{inner});
    Server::builder().add_service(svc).serve(addr).await?;

    Ok(())
}
