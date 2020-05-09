// Copyright Rivtower Technologies LLC.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use clap::Clap;
use git_version::git_version;
use log::{debug, info, warn};

const GIT_VERSION: &str = git_version!(
    args = ["--tags", "--always", "--dirty=-modified"],
    fallback = "unknown"
);
const GIT_HOMEPAGE: &str = "https://github.com/rink1969/cita_ng_executor";

/// network service
#[derive(Clap)]
#[clap(version = "0.1.0", author = "Rivtower Technologies.")]
struct Opts {
    #[clap(subcommand)]
    subcmd: SubCommand,
}

#[derive(Clap)]
enum SubCommand {
    /// print information from git
    #[clap(name = "git")]
    GitInfo,
    /// run this service
    #[clap(name = "run")]
    Run(RunOpts),
}

/// A subcommand for run
#[derive(Clap)]
struct RunOpts {
    /// Sets grpc port of config service.
    #[clap(short = "c", long = "config_port", default_value = "49999")]
    config_port: String,
    /// Sets grpc port of this service.
    #[clap(short = "p", long = "port", default_value = "50002")]
    grpc_port: String,
}

fn main() {
    ::std::env::set_var("RUST_BACKTRACE", "full");

    let opts: Opts = Opts::parse();

    match opts.subcmd {
        SubCommand::GitInfo => {
            println!("git version: {}", GIT_VERSION);
            println!("homepage: {}", GIT_HOMEPAGE);
        }
        SubCommand::Run(opts) => {
            // init log4rs
            log4rs::init_file("executor-log4rs.yaml", Default::default()).unwrap();
            info!("grpc port of config service: {}", opts.config_port);
            info!("grpc port of this service: {}", opts.grpc_port);
            let _ = run(opts);
        }
    }
}

use cita_ng_proto::config::{
    config_service_client::ConfigServiceClient, Endpoint, RegisterEndpointInfo,
};

async fn register_endpoint(
    config_port: String,
    port: String,
) -> Result<bool, Box<dyn std::error::Error>> {
    let config_addr = format!("http://127.0.0.1:{}", config_port);
    let mut client = ConfigServiceClient::connect(config_addr).await?;

    // id of executor service is 2
    let request = Request::new(RegisterEndpointInfo {
        id: 2,
        endpoint: Some(Endpoint {
            hostname: "127.0.0.1".to_owned(),
            port,
        }),
    });

    let response = client.register_endpoint(request).await?;

    Ok(response.into_inner().is_success)
}

use cita_ng_proto::blockchain::CompactBlock;
use cita_ng_proto::common::Hash;
use cita_ng_proto::executor::{
    executor_service_server::ExecutorService, executor_service_server::ExecutorServiceServer,
    CallRequest, CallResponse,
};
use tonic::{transport::Server, Request, Response, Status};

use std::time::Duration;
use tokio::time;

pub struct ExecutorServer {}

impl ExecutorServer {
    fn new() -> Self {
        ExecutorServer {}
    }
}

#[tonic::async_trait]
impl ExecutorService for ExecutorServer {
    async fn exec(&self, request: Request<CompactBlock>) -> Result<Response<Hash>, Status> {
        debug!("exec request: {:?}", request);

        let hash = vec![0u8; 33];
        let reply = Hash { hash };
        Ok(Response::new(reply))
    }
    async fn call(&self, request: Request<CallRequest>) -> Result<Response<CallResponse>, Status> {
        debug!("call request: {:?}", request);

        let value = vec![0u8];
        let reply = CallResponse { value };
        Ok(Response::new(reply))
    }
}

#[tokio::main]
async fn run(opts: RunOpts) -> Result<(), Box<dyn std::error::Error>> {
    let addr_str = format!("127.0.0.1:{}", opts.grpc_port);
    let addr = addr_str.parse()?;

    let executor_server = ExecutorServer::new();

    tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(3));
        loop {
            {
                let ret = register_endpoint(opts.config_port.clone(), opts.grpc_port.clone()).await;
                if ret.is_ok() && ret.unwrap() {
                    info!("register endpoint success!");
                    break;
                }
                warn!("register endpoint failed! Retrying");
            }
            interval.tick().await;
        }
    });

    info!("start grpc server!");
    Server::builder()
        .add_service(ExecutorServiceServer::new(executor_server))
        .serve(addr)
        .await?;

    Ok(())
}
