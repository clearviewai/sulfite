mod args;
mod csv;
mod list;
mod obj;

pub use args::*;
use clap::Parser;
use sulfite::{RetryConfig, S3Client, S3ClientConfig};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let args = Cli::parse();

    // Client is created before dispatch (so --help still triggers client init).
    let client = S3Client::new(
        S3ClientConfig {
            region: args.region.clone(),
            endpoint_url: args.endpoint_url.clone(),
            read_timeout_secs: args.read_timeout,
            ..Default::default()
        },
        RetryConfig {
            max_retries: args.max_retries,
            retriable_client_status_codes: args.retriable_client_status_codes,
            ..Default::default()
        },
    )
    .await;

    match args.command {
        Command::List(a) => list::run_list(client, a).await,
        Command::Head(a) => obj::run_obj(client, ObjCommand::Head(a)).await,
        Command::Download(a) => obj::run_obj(client, ObjCommand::Download(a)).await,
        Command::DownloadMultipart(a) => {
            obj::run_obj(client, ObjCommand::DownloadMultipart(a)).await
        }
        Command::Upload(a) => obj::run_obj(client, ObjCommand::Upload(a)).await,
        Command::UploadMultipart(a) => obj::run_obj(client, ObjCommand::UploadMultipart(a)).await,
        Command::Delete(a) => obj::run_obj(client, ObjCommand::Delete(a)).await,
        Command::Copy(a) => obj::run_obj(client, ObjCommand::Copy(a)).await,
        Command::Restore(a) => obj::run_obj(client, ObjCommand::Restore(a)).await,
        Command::Csv(a) => csv::run_csv(client, a).await,
    }
}
