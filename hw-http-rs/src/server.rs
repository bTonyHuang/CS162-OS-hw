use std::env;
use std::net::{Ipv4Addr, SocketAddrV4};

use crate::args;

use crate::http::*;
use crate::stats::*;

use clap::Parser;
use tokio::net::{TcpListener, TcpStream};
use tokio::spawn;
use tokio::fs::File;
use tokio::fs::metadata;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;

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
        let (socket, addr)=listener.accept().await?;

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
//if encounter any other errors, use log::warn! to print out the error and continue serving requests
async fn handle_socket(mut socket: TcpStream) -> Result<()> {
    //basic info
    let mut status_code=0;
    let mut content_length:u64=0;
    let request_result;
    match parse_request(&mut socket).await{
        Ok(result)=>request_result=result,
        Err(error)=>{
            log::warn!("parse_request error: {}", error);
            return Err(error)
        }
    }

    //directory check
    let mut file_path=format!(".{}",request_result.path);
    let meta_info=metadata(&file_path).await?;
    if meta_info.is_dir() {
        file_path=format_index(&file_path);
    }
    
    //open file check
    let mut target_file;
    match File::open(&file_path).await {
        Ok(file) => {
            status_code=200;
            target_file=file;
        },
        Err(error) => {
            status_code=404;
            log::warn!("Problem opening the file: {:?}", error);
            invalid_request_return(socket,status_code,content_length).await?;
            return Err(error.into())
        }
    };
    
    /*If the file denoted by path exists, serve the file*/
    //get file size
    content_length=meta_info.len();

    start_response(&mut socket,status_code).await?;
    send_header(&mut socket,"Content-Type",get_mime_type(&request_result.path)).await?;
    send_header(&mut socket,"Content-Length",&content_length.to_string()).await?;
    end_headers(&mut socket).await?;

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

// You are free (and encouraged) to add other funtions to this file.
// You can also create your own modules as you see fit.
async fn invalid_request_return(mut socket: TcpStream, status_code: StatusCode, content_length:u64)-> Result<()> {
    start_response(&mut socket,status_code).await?;
    send_header(&mut socket,"Content-Length",&content_length.to_string()).await?;
    end_headers(&mut socket).await?;
    Ok(())
}