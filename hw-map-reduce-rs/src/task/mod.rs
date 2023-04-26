//! If you wish to encapsulate logic related to
//! task and job management, you can do so here.
//!
// You are not required to modify this file.
use crate::rpc::coordinator::*;
use crate::*;

use tokio::time::{Duration, Instant};
use std::collections::HashMap;
use std::collections::VecDeque;

pub struct JobInfo {
    pub job_id: JobId,
    pub files: Vec<String>,  //input files, repeated strings
    pub output_dir: String,
    pub app: String,    //application
    pub n_reduce: u32,
    pub args: Vec<u8>,  //args bytes
    pub done: bool,
    pub failed: bool,
    pub errors: Vec<String>, //error information from worker application

    //hashmap for task and worker for it
    pub task_map: HashMap<TaskNumber,TaskInfo>,

    //queue for map tasks and reduce tasks
    pub task_queue: VecDeque<TaskNumber>,

    //how many map tasks completes
    pub map_complete: u32,

    //how many reduce tasks completes
    pub reduce_complete: u32,

    //shared information for reduce task
    pub map_task_assignments: Vec<MapTaskAssignment>,

    //map tasks number = files number
    pub n_map: u32, 
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
        task_map: HashMap<TaskNumber, TaskInfo>,
        task_queue: VecDeque<TaskNumber>,
        map_complete: u32,
        reduce_complete: u32,
        map_task_assignments: Vec<MapTaskAssignment>,
        n_map: u32,
    ) -> Self{
        Self {
            job_id,
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
        }
    }//end of new
}

pub struct TaskInfo {
    pub job_id: JobId,
    pub task: TaskNumber,
    pub file: String,
    pub reduce: bool,
    pub wait: bool,
    pub worker_id: WorkerId,
}

impl TaskInfo {
    pub fn new(
    job_id: JobId,
    task: TaskNumber,
    file: String,
    reduce: bool,
    wait: bool,
    worker_id: WorkerId,
    ) -> Self{
        Self {
            job_id,
            task,
            file,
            reduce,
            wait,
            worker_id,
        }
    }//end of new
}

#[derive(Clone)]
pub struct WorkerInfo{
    pub job_id: JobId, //0 means idle
    pub task: TaskNumber, 
    pub heartbeat: Instant,
}

impl WorkerInfo{
    pub fn new(
        job_id: JobId,
        task: TaskNumber,
        heartbeat: Instant,
    )->Self{
        Self{
            job_id,
            task,
            heartbeat,
        }
    }//end of new
}