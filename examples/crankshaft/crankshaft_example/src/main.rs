use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use crankshaft::Engine;
use crankshaft::config::backend::Kind;
use crankshaft::config::backend::tes::Config;
use crankshaft::config::backend::tes::http;
use crankshaft::engine::Task;
use crankshaft::engine::task::Execution;
use crankshaft::engine::task::Input;
use crankshaft::engine::task::Output;
use crankshaft::engine::task::Resources;
use crankshaft::engine::task::input;
use crankshaft::engine::task::input::Contents;
use crankshaft::engine::task::output;
use eyre::Context;
use eyre::Result;
use nonempty::NonEmpty;
use url::Url;

/// The environment variable for the output storage account.
const STORAGE_ACCOUNT: &str = "STORAGE_ACCT";

/// The environment variable for a basic auth username.
const USER_ENV: &str = "USER";

/// The environment variable for a basic auth password.
const PASSWORD_ENV: &str = "PASSWORD";

/// The environment variable for the output storage account.
const URL: &str = "URL";

/// Prepares the task to be submitted to Crankshaft.
fn prepare_task() -> Result<Task> {
    let storage_account = std::env::var(STORAGE_ACCOUNT).context(format!(
        "`{}` environment variable must be set!",
        STORAGE_ACCOUNT
    ))?;

    let output_url = format!(
        "file://{}/outputs/crankshaft-tes/H06HDADXX130110.1.ATCACGAT.20k.bam",
        storage_account
    )
    .parse::<Url>()
    .context("could not parse storage account output URL as a URL")?;

    let resources = Resources::builder()
        .cpu(32usize)
        .ram(64.0)
        .preemptible(true)
        .build();

    let mut inputs = NonEmpty::new(
        Input::builder()
            .name("H06HDADXX130110.1.ATCACGAT.20k_reads_1.fastq")
            .path("/data/H06HDADXX130110.1.ATCACGAT.20k_reads_1.fastq")
            .contents(
                Contents::url_from_str(
                    "https://datasettestinputs.blob.core.windows.net/dataset/seq-format\
                     -conversion/NA12878_20k/H06HDADXX130110.1.ATCACGAT.20k_reads_1.fastq",
                )
                .unwrap(),
            )
            .r#type(input::Type::File)
            .build(),
    );

    inputs.extend(vec![
        Input::builder()
            .name("H06HDADXX130110.1.ATCACGAT.20k_reads_2.fastq")
            .path("/data/H06HDADXX130110.1.ATCACGAT.20k_reads_2.fastq")
            .contents(
                Contents::url_from_str(
                    "https://datasettestinputs.blob.core.windows.net/dataset/seq-format\
                     -conversion/NA12878_20k/H06HDADXX130110.1.ATCACGAT.20k_reads_2.fastq",
                )
                .unwrap(),
            )
            .r#type(input::Type::File)
            .build(),
        Input::builder()
            .name("Homo_sapiens_assembly38.fasta")
            .path("/data/Homo_sapiens_assembly38.fasta")
            .contents(
                Contents::url_from_str(
                    "https://datasettestinputs.blob.core.windows.net/dataset/references\
                    /hg38/v0/Homo_sapiens_assembly38.fasta",
                )
                .unwrap(),
            )
            .r#type(input::Type::File)
            .build(),
    ]);

    let outputs = NonEmpty::new(
        Output::builder()
            .name("H06HDADXX130110.1.ATCACGAT.20k.bam")
            .path("/data/H06HDADXX130110.1.ATCACGAT.20k.bam")
            .url(output_url)
            .r#type(output::Type::File)
            .build(),
    );

    let executions = NonEmpty::new(
        Execution::builder()
            .workdir("/data")
            .image("quay.io/biocontainers/bwa:0.7.18--he4a0461_1")
            .args((
                String::from("/bin/sh"),
                vec![
                    String::from("-c"),
                    String::from(
                        "bwa index /data/Homo_sapiens_assembly38.fasta && \
                         bwa mem -t 16 /data/Homo_sapiens_assembly38.fasta \
                           /data/H06HDADXX130110.1.ATCACGAT.20k_reads_1.fastq \
                           /data/H06HDADXX130110.1.ATCACGAT.20k_reads_2.fastq \
                           > /data/H06HDADXX130110.1.ATCACGAT.20k.bam",
                    ),
                ],
            ))
            .build(),
    );

    let task = Task::builder()
        .name("bwa-alignment-task")
        .description("Run BWA MEM on input fastq files")
        .resources(resources)
        .inputs(inputs)
        .outputs(outputs)
        .executions(executions)
        .build();

    Ok(task)
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::builder().url(
        std::env::var(URL)
            .context(format!("the `{}` environment variable must be set", URL))?
            .parse::<Url>()
            .context(format!("failed to parse `{}` as a URL", URL))?,
    );

    let username = std::env::var(USER_ENV).ok();
    let password = std::env::var(PASSWORD_ENV).ok();

    if (username.is_some() && password.is_none()) || (username.is_none() && password.is_some()) {
        panic!("both username and password must be provided for authentication");
    }

    let mut http_config = http::Config::default();

    // If username and password are available, add them to the config.
    if let (Some(username), Some(password)) = (username, password) {
        let credentials = format!("{}:{}", username, password);
        let token = STANDARD.encode(credentials);
        http_config.basic_auth_token = Some(token);
    }

    let config = crankshaft::config::backend::Config::builder()
        .name("tes")
        .kind(Kind::TES(config.http(http_config).build()))
        .max_tasks(1usize)
        .build();

    let engine = Engine::default().with(config).await?;

    let task = prepare_task().context("preparing the task to submit")?;

    let receiver = engine.submit("tes", task).callback;
    engine.run().await;

    println!("Result: {:?}", receiver.await.unwrap());

    Ok(())
}