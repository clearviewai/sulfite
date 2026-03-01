use anyhow::Context;
use sulfite::S3Client;

use crate::ObjCommand;

pub async fn run_obj(client: S3Client, command: ObjCommand) -> anyhow::Result<()> {
    match command {
        ObjCommand::Head(a) => {
            let obj = client.head_object(&a.bucket, &a.key).await?;
            println!("{:?}", obj);
        }
        ObjCommand::Download(a) => {
            let local_path = match &a.local_path {
                Some(p) => p.as_str(),
                None => std::path::Path::new(&a.key)
                    .file_name()
                    .and_then(|os_str| os_str.to_str())
                    .context("key has no file name")?,
            };
            let start_end_offsets = a.start_offset.zip(a.end_offset);
            client
                .download_object(&a.bucket, &a.key, local_path, start_end_offsets)
                .await?;
        }
        ObjCommand::DownloadMultipart(a) => {
            let local_path = match &a.local_path {
                Some(p) => p.as_str(),
                None => std::path::Path::new(&a.key)
                    .file_name()
                    .and_then(|os_str| os_str.to_str())
                    .context("key has no file name")?,
            };
            client
                .download_object_multipart(&a.bucket, &a.key, local_path, Some(a.n_workers))
                .await?;
        }
        ObjCommand::Upload(a) => {
            client
                .upload_object(&a.bucket, &a.key, &a.local_path, a.storage_class.as_deref())
                .await?;
        }
        ObjCommand::UploadMultipart(a) => {
            client
                .upload_object_multipart(
                    &a.bucket,
                    &a.key,
                    &a.local_path,
                    Some(a.n_workers),
                    a.storage_class.as_deref(),
                )
                .await?;
        }
        ObjCommand::Delete(a) => {
            client.delete_object(&a.bucket, &a.key).await?;
        }
        ObjCommand::Copy(a) => {
            client
                .copy_object(
                    &a.src_bucket,
                    &a.src_key,
                    &a.dst_bucket,
                    &a.dst_key,
                    a.storage_class.as_deref(),
                )
                .await?;
        }
        ObjCommand::Restore(a) => {
            client
                .restore_object(&a.bucket, &a.key, a.restore_days, &a.restore_tier)
                .await?;
        }
    }

    Ok(())
}
