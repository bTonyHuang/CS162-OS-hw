use std::env;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::{ffi::OsStr, path::Path};

use crate::args;

use crate::http::*;
use crate::stats::*;

use clap::Parser;
use tokio::net::{TcpListener, TcpStream};
use tokio::spawn;
use tokio::fs;
use tokio::fs::File;
use tokio::fs::metadata;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use anyhow::Result;

pub fn main() -> Result<()> {
    // Configure logging
    // You can print logs (to stderr) using
    // `log::info!`, `log::warn!`, `log::error!`, etc.
    env_logger::Builder::new()
        .filter_level(log::LevelFilter::Info)
        .init();

    // Parse command line arguments
    let args = args::Args::parse();

    // Set the current working directory
    env::set_current_dir(&args.files)?;

    // Print some info for debugging
    log::info!("HTTP server initializing ---------");
    log::info!("Port:\t\t{}", args.port);
    log::info!("Num threads:\t{}", args.num_threads);
    log::info!("Directory:\t\t{}", &args.files);
    log::info!("----------------------------------");

    // Initialize a thread pool that starts running `listen`
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(args.num_threads)
        .build()?
        .block_on(listen(args.port))
}

 /*bind the server socket to the provided port with a host of 0.0.0.0*/
async fn listen(port: u16) -> Result<()> {
    //pub async fn bind<A: ToSocketAddrs>(addr: A) -> Result<TcpListener>
    let listener=TcpListener::bind(format!("0.0.0.0:{}",port)).await?;

    //infinite loop
    loop{
        //pub async fn accept(&self) -> Result<(TcpStream, SocketAddr)>
        let (socket, _)=listener.accept().await?;

        //spawn a task and process each socket concurrently
        //consider threads execute may be terminated by runtime
        spawn(async move{
            if let Err(error)=handle_socket(socket).await {
                log::warn!("handle_socket error: {}",error);
            }
        });
    }
}

// Handles a single connection via `socket`. support GET requests for files and directories
/*use helper functions defined below*/
async fn handle_socket(mut socket: TcpStream) -> Result<()> {
    //basic info
    let mut status_code=404;
    let mut content_length:u64=0;
    let mut content_type="error";
    let parse_result;
    match parse_request(&mut socket).await{
        Ok(result)=>parse_result=result,
        Err(error)=>{
            log::warn!("parse_request error: {}", error);
            start_return(&mut socket,status_code,content_type,content_length).await;
            return Err(error)
        }
    }

    /*Three situations: judge if it is file; judge if the directory exist; judge if the directory contains index.html file*/

    //The request path cannot be used directly as the file path. To access local files, prepend a “.” to the request path.
    let mut file_path= format!(".{}",parse_result.path);

    //get metadata to check file or path
    let mut target_metadata;
    match metadata(&file_path).await{
        Ok(data)=>target_metadata=data,
        Err(error)=>{
            log::warn!("meta_data error: {}", error);
            start_return(&mut socket,status_code,content_type,content_length).await;
            return Err(error.into())
        }
    }

    //directory exist
    if target_metadata.is_dir(){
        let directory_path=format!(".{}",parse_result.path);
        let directory_path=Path::new(&directory_path);
        file_path=format_index(&file_path);
        //check if index.html exist
        match metadata(&file_path).await{
            Ok(data)=>{
                target_metadata=data;
            },
            Err(error)=>{
               /*If the directory does not contain an index.html file,
                respond with an HTML page containing links to all of the immediate children of the directory (similar to ls -1), 
                as well as a link to the parent directory.*/
                status_code=200;
                content_type="text/html";
                start_return(&mut socket,status_code,content_type,content_length).await;
                return_dir_link(directory_path, &mut socket).await?;
                return Ok(());
            }
        }
    }

    //open file check
    let target_file;
    match File::open(&file_path).await {
        Ok(file) => {
            target_file=file;
            status_code=200;
            content_type=get_mime_type(&file_path);
            content_length=target_metadata.len();
        },
        Err(error) => {
            log::warn!("Problem opening the file: {}", error);
            start_return(&mut socket,status_code,content_type,content_length).await;
            return Err(error.into())
        }
    };

    start_return(&mut socket,status_code,content_type,content_length).await;

    return_file(&mut socket,target_file).await?;

    Ok(())
}

// You are free (and encouraged) to add other funtions to this file.
// You can also create your own modules as you see fit.

//begin return request - send header
async fn start_return(socket: &mut TcpStream, status_code: StatusCode, content_type: &str, content_length:u64) {
    proceed_err(start_response(socket,status_code).await);
    proceed_err(send_header(socket,"Content-Type",content_type).await);
    if content_length>0{
        proceed_err(send_header(socket,"Content-Length",&content_length.to_string()).await);
    }

    proceed_err(end_headers(socket).await);
}

//if encounter any other errors, use log::warn! to print out the error and continue serving requests
pub fn proceed_err (result: Result<()>){
    if let Err(error)=result{
        log::warn!("{}",error);
    }
}

//read file to the buffer and write socket from the buffer
//1024 bytes max per cycle
async fn return_file(socket: &mut TcpStream, mut target_file: File)->Result<()> {
    let mut buf: [u8; 1024]=[0;1024];
    while let Ok(nbytes_read) = target_file.read(&mut buf).await {
        // no bytes left
        if nbytes_read == 0 {
            break
        }
        // write to socket
        if let Err(error)=socket.write_all(&buf).await{
            log::warn!("socket write_all error: {}", error);
            return Err(error.into());
        }
        //reset the buf(clean)
        buf=[0;1024];
    }
    Ok(())
}

// one possible implementation of walking a directory only visiting files
async fn return_dir_link(dir: &Path, socket: &mut TcpStream)->Result<()> {
    if dir.is_dir() {
        log::warn!("enter_dir");
        let mut entries_stream = fs::read_dir(dir).await?;
        log::warn!("read_dir");
        loop{
            if let Some(read_entry)=entries_stream.next_entry().await?{
                let filename=read_entry.file_name();
                let filepathbuf=read_entry.path();
                log::warn!("{:?}",filepathbuf.as_os_str());
                let child_link=format_href(filepathbuf.as_os_str().to_str().unwrap(),filename.to_str().unwrap());
                socket.write_all((&child_link).as_bytes()).await?;
            }
            else{
                break;
            }
        }
        //a link to the parent directory.
        let parent_path=dir.parent().unwrap();
        log::warn!("parent_link");
        let parent_link=parent_path.as_os_str().to_str().unwrap();
        let msg=format!("{}\r\n",format_href(parent_link,""));
        socket.write_all((&msg).as_bytes()).await?;
    }
    Ok(())
}