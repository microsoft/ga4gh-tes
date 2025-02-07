//! Submits an example task to an execution service.
//!
//! You can run this with the following command:
//!
//! ```bash
//! export USER="<USER>"
//! export PASSWORD="<PASSWORD>"
//! export RUST_LOG="tes=debug"
//!
//! cargo run --release --features=client,serde --example task-submit <URL>
//! ```

use anyhow::Context;
use anyhow::Result;
use base64::prelude::*;
use tes::v1::client;
use tes::v1::types::Task;
use tes::v1::types::task::Executor;
use tes::v1::types::task::Resources;
use tes::v1::types::task::Input;
use tes::v1::types::task::Output;
use tes::v1::client::tasks::View;
use tes::v1::types::responses::task::Response;
use tes::v1::types::task::State;
use tracing_subscriber::EnvFilter;
use ordered_float::OrderedFloat;
use std::time::Duration;
use tokio::time::sleep;

/// The environment variable for a basic auth username.
const USER_ENV: &str = "USER";

/// The environment variable for a basic auth password.
const PASSWORD_ENV: &str = "PASSWORD";

/// The environment variable for a storage account.
const STORAGE_ACCT_ENV: &str = "STORAGE_ACCT";

/// The final states that indicate task completion.
const FINAL_STATES: &[&str] = &["Complete", "ExecutorError", "SystemError", "Canceled"];

fn state_to_str(state: &State) -> &'static str {
    match state {
        State::Complete => "Complete",
        State::ExecutorError => "ExecutorError",
        State::SystemError => "SystemError",
        State::Canceled => "Canceled",
        _ => "Unknown",
    }
}



#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let url = std::env::args().nth(1).expect("url to be present");

    let mut builder = client::Builder::default()
        .url_from_string(url)
        .expect("url could not be parsed");

    let username = std::env::var(USER_ENV).ok();
    let password = std::env::var(PASSWORD_ENV).ok();
    let storage_acct = std::env::var(STORAGE_ACCT_ENV).ok();

    if (username.is_some() && password.is_none()) || (username.is_none() && password.is_some()) {
        panic!("${USER_ENV} and ${PASSWORD_ENV} must both be set to use basic auth");
    }


    if storage_acct.is_none() {
        panic!("${STORAGE_ACCT_ENV} must  be set to use basic TES");
    }

    if let Some(username) = username {
        let credentials = format!("{}:{}", username, password.unwrap());
        let encoded = BASE64_STANDARD.encode(credentials);
        builder = builder.insert_header("Authorization", format!("Basic {}", encoded));
    }

    let storage_acct = storage_acct.unwrap_or_else(|| "default-storage".to_string());
    let output_url = format!("/{}/outputs/crankshaft-tes/H06HDADXX130110.1.ATCACGAT.20k.bam", storage_acct);

    let client = builder.try_build().expect("could not build client");

    let task = Task {
        name: Some(String::from("bwa-alignment-task")),
        description: Some(String::from("Run BWA MEM on input fastq files")),
        resources: Some(Resources {
            cpu_cores: Some(32),
            ram_gb: Some(OrderedFloat(64.0)),
            preemptible: Some(true),
            ..Default::default()
        }),
        executors: vec![Executor {
            image: String::from("quay.io/biocontainers/bwa:0.7.18--he4a0461_1"),
            command: vec![
                String::from("/bin/sh"),
                String::from("-c"),
                String::from("bwa index /data/Homo_sapiens_assembly38.fasta && bwa mem -t 16 /data/Homo_sapiens_assembly38.fasta /data/H06HDADXX130110.1.ATCACGAT.20k_reads_1.fastq /data/H06HDADXX130110.1.ATCACGAT.20k_reads_2.fastq > /data/H06HDADXX130110.1.ATCACGAT.20k.bam"),
            ],
            workdir: Some(String::from("/data")),
            ..Default::default()
        }],
        inputs: Some(vec![
            Input {
                name: Some(String::from("H06HDADXX130110.1.ATCACGAT.20k_reads_1.fastq")),
                path: String::from("/data/H06HDADXX130110.1.ATCACGAT.20k_reads_1.fastq"),
                url: Some(String::from("https://datasettestinputs.blob.core.windows.net/dataset/seq-format-conversion/NA12878_20k/H06HDADXX130110.1.ATCACGAT.20k_reads_1.fastq")),
                ..Default::default()
            },
            Input {
                name: Some(String::from("H06HDADXX130110.1.ATCACGAT.20k_reads_2.fastq")),
                path: String::from("/data/H06HDADXX130110.1.ATCACGAT.20k_reads_2.fastq"),
                url: Some(String::from("https://datasettestinputs.blob.core.windows.net/dataset/seq-format-conversion/NA12878_20k/H06HDADXX130110.1.ATCACGAT.20k_reads_2.fastq")),
                ..Default::default()
            },
            Input {
                name: Some(String::from("Homo_sapiens_assembly38.fasta")),
                path: String::from("/data/Homo_sapiens_assembly38.fasta"),
                url: Some(String::from("https://datasettestinputs.blob.core.windows.net/dataset/references/hg38/v0/Homo_sapiens_assembly38.fasta")),
                ..Default::default()
            }]),
        outputs: Some(vec![Output {
            name: Some(String::from("H06HDADXX130110.1.ATCACGAT.20k.bam")),
            path: String::from("/data/H06HDADXX130110.1.ATCACGAT.20k.bam"),
            url:  output_url,
            ..Default::default()
        }]),
        ..Default::default()
    };

    //let response = client.create_task(task).await.context("submitting a task")?;
    //let task_id = response.id.as_ref().ok_or_else(|| anyhow::anyhow!("Task ID missing in response"))?.to_string();


    let response = client
    .create_task(task)
    .await
    .context("submitting a task")?;

    // Extract the task ID
    let task_id = response.id.clone(); 

    // Print the task ID
    println!("Task submitted successfully! Task ID: {}", task_id);

    // Loop 
    loop {
        sleep(Duration::from_secs(10)).await;
    
        let task_status = client
            .get_task(&task_id, View::Minimal)
            .await
            .context("getting task status")?;
    
    
        if let Response::Minimal(minimal_task) = task_status {
            if let Some(state) = &minimal_task.state {
                println!("Task State: {:?}", state);
                if FINAL_STATES.contains(&state_to_str(&state))  {
                    println!("Task reached a final state: {:?}", state);
                    break;
                }
            }
        }
    }
    
    // Now Ok(()) is reachable
    Ok(())
    
}