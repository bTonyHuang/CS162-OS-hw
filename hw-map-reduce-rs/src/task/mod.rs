//! If you wish to encapsulate logic related to
//! task and job management, you can do so here.
//!
// You are not required to modify this file.
use crate::rpc::coordinator::*;
use crate::*;

use tokio::time::{Duration, Instant};
use std::collections::HashMap;

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
        }
    }//end of new
}

pub struct TaskInfo {
    pub job_id: JobId,
    pub output_dir: String,
    pub app: String,
    pub task: TaskNumber,
    pub file: String,
    pub n_reduce: u32,
    pub n_map: u32, //map tasks number = files number
    pub reduce: bool,
    pub wait: bool,
    pub map_task_assignments: Vec<MapTaskAssignment>,
    pub args: Vec<u8>,
    pub worker_id: WorkerId,
}

impl TaskInfo {
    pub fn new(
    job_id: JobId,
    output_dir: String,
    app: String,
    task: TaskNumber,
    file: String,
    n_reduce: u32,
    n_map: u32, //map tasks number = files number
    reduce: bool,
    wait: bool,
    map_task_assignments: Vec<MapTaskAssignment>,
    args: Vec<u8>,
    worker_id: WorkerId,
    ) -> Self{
        Self {
            job_id,
            output_dir,
            app,
            task,
            file,
            n_reduce,
            n_map,
            reduce,
            wait,
            map_task_assignments,
            args,
            worker_id,
        }
    }//end of new

    pub fn clone(&self)->TaskInfo{
        TaskInfo::new(
            self.job_id,
            self.output_dir.clone(),
            self.app.clone(),
            self.task,
            self.file.clone(),
            self.n_reduce,
            self.n_map,
            self.reduce,
            self.wait,
            self.map_task_assignments.clone(),
            self.args.clone(),
            self.worker_id,
        )
    }
}

pub struct WorkerInfo{
    pub job_id: JobId,
    pub task: TaskNumber, //0 means idle
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