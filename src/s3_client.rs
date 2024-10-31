use crate::retry_strategy::RetryStrategy;
use crate::utils::generate_random_hex;
use anyhow::{anyhow, bail, Context, Result};
use aws_config::{timeout::TimeoutConfig, Region};
use aws_credential_types::Credentials;
use aws_sdk_s3::{
    error::SdkError,
    primitives::{ByteStream, ByteStreamError, Length},
    types::{
        CompletedMultipartUpload, CompletedPart, GlacierJobParameters, RestoreRequest,
        StorageClass, Tier,
    },
    Client as AWSS3Client, Error as AWSS3Error,
};
use bytes::Bytes;
#[allow(unused_imports)]
use log::{debug, error, info, warn};
use partial_application::partial;
use std::sync::Arc;
use thiserror::Error;
use tokio::io::BufWriter;
use tokio::{
    io::{AsyncSeekExt, AsyncWriteExt},
    sync::Semaphore,
};
use tokio_retry::strategy::jitter;
use tokio_retry::RetryIf;

const DEFAULT_REGION: &str = "us-east-1"; // AWS default
const DEFAULT_TIMEOUT: u64 = 60; // boto default
const DEFAULT_MAX_RETRIES: usize = 3; // emulate AWS SDK default
const UPLOAD_BYTESTREAM_BUFFER_SIZE: usize = 1024 * 1024; // 1MB
const MULTIPART_CHUNK_SIZE: u64 = 1024 * 1024 * 20; // 20MB when filesize < 200GB
const MULTIPART_MAX_CHUNKS: u64 = 10000; // API limit

#[derive(Clone, Debug)]
pub struct ObjectInfo {
    pub key: String,
    pub size: usize,
    pub timestamp: String,
    pub storage_class: Option<String>,
    pub restore_status: Option<String>,
}

#[derive(Clone, Debug)]
pub struct CommonPrefixInfo {
    pub prefix: String,
}

pub type ListObjectsV2PaginateResp = Result<(Vec<ObjectInfo>, Vec<CommonPrefixInfo>)>;

#[derive(Error, Debug)]
pub enum S3Error {
    #[error("{} [TimeoutError]", .0)]
    TimeoutError(String),
    # [error("{} [DispatchFailure]", .0)]
    DispatchFailure(String),
    #[error("{} [ResponseError]", .0)]
    ResponseError(String),
    #[error("{} [WasabiConnectionLimitExceeded]", .0)]
    WasabiConnectionLimitExceeded(String),
    #[error("{} [AccessDenied]", .0)]
    AccessDenied(String),
    #[error("{} [TooManyRequests]", .0)]
    TooManyRequests(String),
    #[error("{} [InternalError]", .0)]
    InternalError(String),
    #[error("{} [S3Error - {}]", .0, .1)]
    S3Error(String, AWSS3Error),
    #[error("{} [ByteStreamError - {}]", .0, .1)]
    ByteStreamError(String, ByteStreamError),
}

fn map_sdk_error<E>(context: String, e: SdkError<E>) -> S3Error
where
    AWSS3Error: From<SdkError<E>>,
    E: std::fmt::Debug,
{
    match &e {
        SdkError::TimeoutError(_) => S3Error::TimeoutError(context),
        SdkError::DispatchFailure(dispatch_error) => {
            debug!(
                "is_io: {} is_timeout: {} is_user: {} is_other: {} {:?}",
                dispatch_error.is_io(),
                dispatch_error.is_timeout(),
                dispatch_error.is_user(),
                dispatch_error.is_other(),
                dispatch_error
            );
            S3Error::DispatchFailure(context)
        }
        SdkError::ResponseError(response_error) => {
            if let Some(bytes) = response_error.raw().body().bytes() {
                if let Ok(raw_content) = String::from_utf8(bytes.to_vec()) {
                    debug!("{}", raw_content);
                }
            }
            S3Error::ResponseError(context)
        }
        SdkError::ServiceError(service_error) => {
            if let Some(bytes) = service_error.raw().body().bytes() {
                if let Ok(raw_content) = String::from_utf8(bytes.to_vec()) {
                    debug!("{}", raw_content);
                }
            }

            let status_code = service_error.raw().status().as_u16();
            if status_code == 403 {
                // bypass due to wasabi's impl
                if let Some(bytes) = service_error.raw().body().bytes() {
                    if let Ok(raw_content) = String::from_utf8(bytes.to_vec()) {
                        // debug!("{}", raw_content);
                        if raw_content.contains("ConnectionLimitExceeded") {
                            return S3Error::WasabiConnectionLimitExceeded(context);
                        }
                    }
                }
                S3Error::AccessDenied(context)
                // S3Error::S3Error(context, AWSS3Error::from(e)) // would be unhandled error (AccessDenied) in current SDK
            } else if status_code == 429 {
                S3Error::TooManyRequests(context)
            } else if status_code >= 500 {
                S3Error::InternalError(context)
            } else {
                S3Error::S3Error(context, AWSS3Error::from(e))
            }
        }
        _ => {
            error!("{context} {:?}", e);
            S3Error::S3Error(context, AWSS3Error::from(e))
        }
    }
}

fn map_bytestream_error(context: String, e: ByteStreamError) -> S3Error {
    debug!("{context} {:?}", e);
    S3Error::ByteStreamError(context, e)
}

fn should_retry(e: &anyhow::Error) -> bool {
    if let Some(e) = e.downcast_ref::<S3Error>() {
        match e {
            S3Error::TimeoutError(_)
            | S3Error::DispatchFailure(_)
            | S3Error::ResponseError(_)
            | S3Error::WasabiConnectionLimitExceeded(_)
            // | S3Error::AccessDenied(_) // sometimes rate limit is 403 for wasabi
            | S3Error::TooManyRequests(_)
            | S3Error::InternalError(_)
            | S3Error::ByteStreamError(_, _)
             => {
                warn!("RetryIf: {}. Retrying...", e);
                true
            }
            _ => {
                // other S3Error errors
                debug!("RetryIf: {}. Not retrying...", e);
                false
            }
        }
    } else {
        // other errors
        false
    }
}

#[derive(Debug, Clone)]
pub struct S3Client {
    client: AWSS3Client,
    retry_strategy: RetryStrategy,
    max_retries: usize,
}

impl S3Client {
    pub async fn new(
        region: Option<String>,
        endpoint_url: Option<String>,
        profile_name: Option<String>,
        access_secret_session_tuple: Option<(String, String, Option<String>)>,
        operation_attempt_timeout_secs: Option<u64>,
        retry_strategy: RetryStrategy,
        max_retries: Option<usize>,
    ) -> Self {
        let mut config_loader = aws_config::from_env();
        if let Some(region) = region {
            config_loader = config_loader.region(Region::new(region))
        }

        if let Some(endpoint_url) = endpoint_url {
            config_loader = config_loader.endpoint_url(endpoint_url);
        }

        if let Some(profile_name) = profile_name {
            config_loader = config_loader.profile_name(profile_name);
        }
        if let Some((access_key, secret_key, session_token)) = access_secret_session_tuple {
            config_loader = config_loader.credentials_provider(Credentials::from_keys(
                access_key,
                secret_key,
                session_token,
            ));
        }

        config_loader = config_loader.timeout_config(
            TimeoutConfig::builder()
                .operation_attempt_timeout(std::time::Duration::from_secs(
                    operation_attempt_timeout_secs.unwrap_or(DEFAULT_TIMEOUT),
                ))
                .build(),
        );

        let sdk_config = config_loader.load().await;
        let mut config_builder = aws_sdk_s3::config::Builder::from(&sdk_config);
        if sdk_config.region().is_none() {
            info!(
                "Can't resolve region. Using default region: {}",
                DEFAULT_REGION
            );
            config_builder = config_builder.region(Region::new(DEFAULT_REGION));
        }
        config_builder = config_builder.force_path_style(true); // this allows http://minio:11000 style endpoint_url

        S3Client {
            client: AWSS3Client::from_conf(config_builder.build()),
            retry_strategy,
            max_retries: max_retries.unwrap_or(DEFAULT_MAX_RETRIES),
        }
    }

    pub fn from_aws_s3_client(
        client: AWSS3Client,
        retry_strategy: RetryStrategy,
        max_retries: Option<usize>,
    ) -> Self {
        S3Client {
            client,
            retry_strategy,
            max_retries: max_retries.unwrap_or(DEFAULT_MAX_RETRIES),
        }
    }

    async fn _list_objects_v2_paginate(
        &self,
        bucket: &str,
        prefix: &str,
        delimiter: Option<&str>,
    ) -> ListObjectsV2PaginateResp {
        let mut builder = self.client.list_objects_v2().bucket(bucket).prefix(prefix);

        if let Some(delimiter) = delimiter {
            builder = builder.delimiter(delimiter);
        }

        let mut pagination_stream = builder.into_paginator().send();

        let mut objects: Vec<ObjectInfo> = vec![];
        let mut common_prefixes: Vec<CommonPrefixInfo> = vec![];
        while let Some(item) = pagination_stream
            .try_next()
            .await
            .map_err(partial!(map_sdk_error => format!("<list_objects_v2_paginate> bucket={bucket} prefix={prefix}"), _))?
        {
            item.contents().into_iter().try_for_each(|object| {
                objects.push(ObjectInfo {
                    key: object.key().context("No such key!")?.into(),
                    size: object.size().context("No such size!")? as usize,
                    timestamp: object
                        .last_modified()
                        .context("No such timestamp!")?
                        .to_string(),
                    storage_class: object.storage_class().map(|sc| sc.as_str().to_owned()),
                    // restore_status is not populated for some reason - we leave the code placeholder for now
                    restore_status: object.restore_status().map(|rs| {
                        let is_restoring =
                        if let Some(is_restoring) = rs.is_restore_in_progress {
                            if is_restoring {
                                "Restoring"
                            } else {
                                "Restored"
                            }
                        } else {
                            "N/A"
                        };
                    let ts = rs.restore_expiry_date().map(|red| red.to_string());
                        format!("Restoring {} (Expires: {})", is_restoring, ts.unwrap_or("N/A".to_owned()))
                    }),
                });
                anyhow::Ok(())
            })?;
            item.common_prefixes()
                .into_iter()
                .try_for_each(|common_prefix| {
                    common_prefixes.push(CommonPrefixInfo {
                        prefix: common_prefix.prefix().context("No such prefix!")?.into(),
                    });
                    anyhow::Ok(())
                })?;
        }
        Ok((objects, common_prefixes))
    }

    pub async fn list_objects_v2_paginate(
        &self,
        bucket: &str,
        prefix: &str,
        delimiter: Option<&str>,
    ) -> ListObjectsV2PaginateResp {
        let (objects, common_prefixes) = RetryIf::spawn(
            self.retry_strategy
                .clone()
                .map(jitter)
                .take(self.max_retries),
            || async {
                self._list_objects_v2_paginate(bucket, prefix, delimiter)
                    .await
            },
            should_retry,
        )
        .await?;

        debug!(
            "Prefix {}: Found {} objects and {} common prefixes.",
            prefix,
            objects.len(),
            common_prefixes.len()
        );

        Ok((objects, common_prefixes))
    }

    pub async fn head_object(&self, bucket: &str, key: &str) -> Result<ObjectInfo> {
        let resp = RetryIf::spawn(
            self.retry_strategy.clone().map(jitter).take(self.max_retries),
            || async {
                self
                    .client
                    .head_object()
                    .bucket(bucket)
                    .key(key)
                    .send()
                    .await
                    .map_err(partial!(map_sdk_error => format!("<head_object> bucket={bucket} key={key}"), _))
                    .map_err(|e| e.into())
            },
            should_retry,
        )
        .await?;

        let object_info = ObjectInfo {
            key: key.into(),
            size: resp.content_length().context("No info!")? as usize,
            timestamp: resp.last_modified().context("No info!")?.to_string(),
            storage_class: resp.storage_class().map(|sc| sc.as_str().to_owned()),
            restore_status: resp.restore().map(|rs| rs.to_owned()),
        };
        debug!("Found object with key={}", key);
        debug!("Content length: {}", object_info.size);
        debug!("Last modified: {}", object_info.timestamp);
        debug!("Storage class: {:?}", object_info.storage_class);
        debug!("Content type: {}", resp.content_type().context("No info!")?);

        Ok(object_info)
    }

    async fn _get_object(
        &self,
        bucket: &str,
        key: &str,
        start_end_offsets: Option<(usize, usize)>,
    ) -> Result<(ObjectInfo, Vec<u8>)> {
        let mut builder = self.client.get_object().bucket(bucket).key(key);

        if let Some(start_end_offsets) = start_end_offsets {
            if start_end_offsets.1 <= start_end_offsets.0 {
                bail!("Invalid start_end_offsets, non-positive slice!");
            }
            let range = format!(
                "bytes={}-{}",
                start_end_offsets.0,
                // end_offset is provided exclusive but the "end" of "bytes=start,end" is inclusive
                start_end_offsets.1 - 1
            );
            builder = builder.range(range);
        }

        let resp = builder.send().await.map_err(
            partial!(map_sdk_error => format!("<get_object> bucket={bucket} key={key}"), _),
        )?;

        let object_info = ObjectInfo {
            key: key.into(),
            size: resp.content_length().context("No info!")? as usize,
            timestamp: resp.last_modified().context("No info!")?.to_string(),
            storage_class: resp.storage_class().map(|sc| sc.as_str().to_owned()),
            restore_status: resp.restore().map(|rs| rs.to_owned()),
        };

        debug!("Found object with key={}", key);
        debug!("Content length: {}", object_info.size);
        debug!("Last modified: {}", object_info.timestamp);
        debug!("Storage class: {:?}", object_info.storage_class);
        debug!("Content type: {}", resp.content_type().context("No info!")?);

        let content = resp
            .body
            .collect()
            .await
            .map_err(partial!(map_bytestream_error => format!("<get_object> bucket={bucket} key={key}"), _))?
            .into_bytes().to_vec();
        Ok((object_info, content))
    }

    pub async fn get_object(
        &self,
        bucket: &str,
        key: &str,
        start_end_offsets: Option<(usize, usize)>,
    ) -> Result<(ObjectInfo, Vec<u8>)> {
        let (object_info, content) = RetryIf::spawn(
            self.retry_strategy
                .clone()
                .map(jitter)
                .take(self.max_retries),
            || async { self._get_object(bucket, key, start_end_offsets).await },
            should_retry,
        )
        .await?;

        Ok((object_info, content))
    }

    async fn _download_object(
        &self,
        bucket: &str,
        key: &str,
        local_path: &str,
        start_end_offsets: Option<(usize, usize)>,
    ) -> Result<ObjectInfo> {
        let mut builder = self.client.get_object().bucket(bucket).key(key);

        if let Some(start_end_offsets) = start_end_offsets {
            if start_end_offsets.1 <= start_end_offsets.0 {
                bail!("Invalid start_end_offsets, non-positive slice!");
            }
            let range = format!(
                "bytes={}-{}",
                start_end_offsets.0,
                // end_offset is provided exclusive but the "end" of "bytes=start,end" is inclusive
                start_end_offsets.1 - 1
            );
            builder = builder.range(range);
        }

        let mut resp = builder.send().await.map_err(
            partial!(map_sdk_error => format!("<download_object> bucket={bucket} key={key}"), _),
        )?;

        let object_info = ObjectInfo {
            key: key.into(),
            size: resp.content_length().context("No info!")? as usize,
            timestamp: resp.last_modified().context("No info!")?.to_string(),
            storage_class: resp.storage_class().map(|sc| sc.as_str().to_owned()),
            restore_status: resp.restore().map(|rs| rs.to_owned()),
        };

        let local_path = local_path.to_owned();
        let timestamp = resp.last_modified().context("No info!")?.clone();

        let random_suffix = generate_random_hex(8);
        let local_path_tmp = format!("{local_path}.{random_suffix}");
        let mut file = BufWriter::with_capacity(
            1024 * 1024, // 1MB buffer
            tokio::fs::File::create(&local_path_tmp).await?,
        );

        // let mut reader = BufReader::with_capacity(
        //     1024 * 1024, // 1MB buffer
        //     resp.body
        //         .into_async_read()
        // );
        // tokio::io::copy(&mut reader, &mut file).await?;

        while let Some(bytes) =
            resp.body.try_next().await.map_err(
                partial!(map_bytestream_error => format!("<download_object> bucket={bucket} key={key}"), _),
            ).map_err(|e| {
                // clean up
                let _ = std::fs::remove_file(&local_path_tmp);
                e
            })?
        {
            file.write_all(&bytes).await?;
        }
        file.flush().await?;

        // set the file mtime according to object timestamp
        tokio::task::spawn_blocking(move || {
            filetime::set_file_mtime(
                &local_path_tmp,
                filetime::FileTime::from_unix_time(timestamp.secs(), timestamp.subsec_nanos()),
            )?;
            std::fs::rename(&local_path_tmp, &local_path)?;
            anyhow::Ok(())
        })
        .await??;

        Ok(object_info)
    }

    pub async fn download_object(
        &self,
        bucket: &str,
        key: &str,
        local_path: &str,
        start_end_offsets: Option<(usize, usize)>,
    ) -> Result<ObjectInfo> {
        let obj = RetryIf::spawn(
            self.retry_strategy
                .clone()
                .map(jitter)
                .take(self.max_retries),
            || async {
                self._download_object(bucket, key, local_path, start_end_offsets)
                    .await
            },
            should_retry,
        )
        .await?;

        debug!("Downloaded from s3://{}/{} to {}", bucket, key, local_path);

        Ok(obj)
    }

    pub async fn download_object_multipart(
        &self,
        bucket: &str,
        key: &str,
        local_path: &str,
        n_downloaders: Option<usize>,
        pb: Option<&indicatif::ProgressBar>,
    ) -> Result<ObjectInfo> {
        // head first
        let resp = self
            .client
            .head_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(partial!(map_sdk_error => format!("<download_object_multipart> bucket={bucket} key={key}"), _))?;

        let file_size = resp.content_length().context("No info!")? as u64;
        let timestamp = resp.last_modified().context("No info!")?.to_owned();

        let object_info = ObjectInfo {
            key: key.into(),
            size: file_size as usize,
            timestamp: timestamp.to_string(),
            storage_class: resp.storage_class().map(|sc| sc.as_str().to_owned()),
            restore_status: resp.restore().map(|rs| rs.to_owned()),
        };

        if file_size == 0 {
            let local_path_ = local_path.to_owned();
            tokio::task::spawn_blocking(move || {
                std::fs::File::create(&local_path_)?;
                filetime::set_file_mtime(
                    &local_path_,
                    filetime::FileTime::from_unix_time(timestamp.secs(), timestamp.subsec_nanos()),
                )?;
                anyhow::Ok(())
            })
            .await??;
            debug!("Created blank file at {local_path}");
            return Ok(object_info);
        }

        let mut chunk_count = (file_size / MULTIPART_CHUNK_SIZE) + 1;
        let mut size_of_last_chunk = file_size % MULTIPART_CHUNK_SIZE;
        if size_of_last_chunk == 0 {
            size_of_last_chunk = MULTIPART_CHUNK_SIZE;
            chunk_count -= 1;
        }
        debug!("Chunk count: {}", chunk_count);
        pb.map(|pb| pb.set_length(chunk_count as u64));

        let random_suffix = generate_random_hex(8);
        let local_path_tmp = format!("{local_path}.{random_suffix}");
        let local_path_tmp_ = local_path_tmp.clone();
        tokio::fs::File::create(&local_path_tmp).await?;

        // parallel download
        let sem = Arc::new(Semaphore::new(n_downloaders.unwrap_or(1)));
        let mut join_set = tokio::task::JoinSet::new();
        for chunk_index in 0..chunk_count {
            let retry_strategy = self.retry_strategy.clone();
            let max_retries = self.max_retries;
            let client = self.client.clone();
            let local_path_tmp = local_path_tmp_.clone();
            let bucket = bucket.to_string();
            let key = key.to_string();
            let pb = pb.map(|pb| pb.clone());

            let permit = Arc::clone(&sem).acquire_owned().await;
            join_set.spawn(async move {
                let _permit = permit; // consume semaphore

                let this_chunk = if chunk_count - 1 == chunk_index {
                    size_of_last_chunk
                } else {
                    MULTIPART_CHUNK_SIZE
                };

                let start_offset = chunk_index * MULTIPART_CHUNK_SIZE;
                let end_offset = start_offset + this_chunk;

                RetryIf::spawn(
                    retry_strategy.clone().map(jitter).take(max_retries),
                    || async {
                        // end_offset is provided exclusive but the "end" of "bytes=start,end" is inclusive
                        let range = format!("bytes={}-{}", start_offset, end_offset - 1);
                        debug!("Getting chunk {} with range: {}", chunk_index, range);
                        let mut resp = client
                            .get_object()
                            .bucket(&bucket)
                            .key(&key)
                            .range(range)
                            .send()
                            .await.map_err(
                                partial!(map_sdk_error => format!("<download_object_multipart> bucket={bucket} key={key} download_chunk_index={chunk_index}"), _),
                            )?;
                        debug!("Done getting chunk {}", chunk_index);

                        let mut file = BufWriter::with_capacity(
                            1024 * 1024, // 1MB buffer
                            tokio::fs::OpenOptions::new()
                                .write(true)
                                .open(&local_path_tmp).await?,
                        );
                        file.seek(std::io::SeekFrom::Start(start_offset)).await?;
                        debug!("Streaming chunk {} to file", chunk_index);

                        while let Some(bytes) =
                            resp.body.try_next().await.map_err(partial!(map_bytestream_error => format!("<download_object_multipart> bucket={bucket} key={key} download_chunk_index={chunk_index}"), _))? 
                        {
                            file.write_all(&bytes).await?;
                        }
                        file.flush().await?;
                        debug!("Done streaming chunk {} to file", chunk_index);

                        anyhow::Ok(())
                    },
                    should_retry,
                )
                .await?;

                pb.map(|pb| {
                    pb.inc(1);
                });
                anyhow::Ok(())
            });
        }

        // collect results
        let mut res_ = Ok(object_info);
        while let Some(res) = join_set.join_next().await {
            if let Ok(res) = res {
                if let Err(e) = res {
                    // abort all tasks and break on first error encountered
                    join_set.abort_all();
                    res_ = Err(e);
                    break;
                }
            } else {
                // canceled
                res_ = Err(anyhow!("Multipart download of {local_path} was canceled!"));
                break;
            }
        }

        if let Ok(_) = res_ {
            let local_path_ = local_path.to_owned();
            // set the file mtime according to object timestamp
            tokio::task::spawn_blocking(move || {
                filetime::set_file_mtime(
                    &local_path_tmp,
                    filetime::FileTime::from_unix_time(timestamp.secs(), timestamp.subsec_nanos()),
                )?;
                std::fs::rename(&local_path_tmp, &local_path_)?;
                anyhow::Ok(())
            })
            .await??;
            debug!(
                "Downloaded multipart from s3://{}/{} to {}",
                bucket, key, local_path
            );
        } else {
            let _ = tokio::fs::remove_file(&local_path_tmp).await;
            error!("Download of {local_path} failed! Not finalizing the file.")
        }

        res_
    }

    async fn _put_object(
        &self,
        bucket: &str,
        key: &str,
        content: Bytes,
        storage_class: Option<&str>,
    ) -> Result<()> {
        let body = ByteStream::from(content);
        let mut builder = self.client.put_object().bucket(bucket).key(key).body(body);

        if let Some(storage_class) = storage_class {
            builder = builder.storage_class(StorageClass::from(storage_class));
        }

        builder.send().await.map_err(
            partial!(map_sdk_error => format!("<put_object> bucket={bucket} key={key}"), _),
        )?;

        Ok(())
    }

    pub async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        content: &[u8],
        storage_class: Option<&str>,
    ) -> Result<()> {
        let content = Bytes::from(content.to_vec());
        RetryIf::spawn(
            self.retry_strategy
                .clone()
                .map(jitter)
                .take(self.max_retries),
            || async {
                self._put_object(bucket, key, content.clone(), storage_class)
                    .await
            },
            should_retry,
        )
        .await?;

        debug!("Put from memory to s3://{}/{}", bucket, key);

        Ok(())
    }

    async fn _upload_object(
        &self,
        bucket: &str,
        key: &str,
        local_path: &str,
        storage_class: Option<&str>,
    ) -> Result<()> {
        let body = ByteStream::read_from()
            .path(local_path)
            .buffer_size(UPLOAD_BYTESTREAM_BUFFER_SIZE)
            .build()
            .await
            .map_err(
            partial!(map_bytestream_error => format!("<upload_object> bucket={bucket} key={key}"), _),
        )?;
        let mut builder = self.client.put_object().bucket(bucket).key(key).body(body);

        if let Some(storage_class) = storage_class {
            builder = builder.storage_class(StorageClass::from(storage_class));
        }

        builder.send().await.map_err(
            partial!(map_sdk_error => format!("<upload_object> bucket={bucket} key={key}"), _),
        )?;

        Ok(())
    }

    pub async fn upload_object(
        &self,
        bucket: &str,
        key: &str,
        local_path: &str,
        storage_class: Option<&str>,
    ) -> Result<()> {
        RetryIf::spawn(
            self.retry_strategy
                .clone()
                .map(jitter)
                .take(self.max_retries),
            || async {
                self._upload_object(bucket, key, local_path, storage_class)
                    .await
            },
            should_retry,
        )
        .await?;

        debug!("Uploaded from {} to s3://{}/{}", local_path, bucket, key);

        Ok(())
    }

    pub async fn upload_object_multipart(
        &self,
        bucket: &str,
        key: &str,
        local_path: &str,
        n_uploaders: Option<usize>,
        storage_class: Option<&str>,
        pb: Option<&indicatif::ProgressBar>,
    ) -> Result<()> {
        let file_size = tokio::fs::metadata(local_path).await?.len();
        if file_size == 0 {
            self.put_object(bucket, key, vec![].as_slice(), storage_class)
                .await?;
            return Ok(());
        }

        // create the multipart upload
        let create_multipart_upload_output = RetryIf::spawn(
            self.retry_strategy
                .clone()
                .map(jitter)
                .take(self.max_retries),
            || async {
                let mut builder = self
                    .client
                    .create_multipart_upload()
                    .bucket(bucket)
                    .key(key);

                if let Some(storage_class) = storage_class {
                    builder = builder.storage_class(StorageClass::from(storage_class));
                }

                builder
                    .send()
                    .await
                    .map_err(partial!(map_sdk_error => format!("<upload_object_multipart> bucket={bucket} key={key}"), _))
                    .map_err(|e| e.into())
            },
            should_retry,
        )
        .await?;

        let upload_id = create_multipart_upload_output
            .upload_id()
            .context("No upload id!")?;

        // prepare the file to upload - chunking scheme
        let mut multipart_chunk_size = MULTIPART_CHUNK_SIZE;
        if file_size > MULTIPART_CHUNK_SIZE * MULTIPART_MAX_CHUNKS {
            multipart_chunk_size = file_size / (MULTIPART_MAX_CHUNKS - 1);
            debug!(
                "File size larger than 200GB. Using adaptive chunk size {multipart_chunk_size}."
            );
        }

        let mut chunk_count = (file_size / multipart_chunk_size) + 1;
        let mut size_of_last_chunk = file_size % multipart_chunk_size;
        if size_of_last_chunk == 0 {
            size_of_last_chunk = multipart_chunk_size;
            chunk_count -= 1;
        }
        debug!("Chunk count: {}", chunk_count);
        pb.map(|pb| pb.set_length(chunk_count as u64));

        // parallel upload
        let sem = Arc::new(Semaphore::new(n_uploaders.unwrap_or(1)));
        let mut join_set = tokio::task::JoinSet::new();
        for chunk_index in 0..chunk_count {
            let retry_strategy = self.retry_strategy.clone();
            let max_retries = self.max_retries;
            let client = self.client.clone();
            let local_path = local_path.to_string();
            let bucket = bucket.to_string();
            let key = key.to_string();
            let upload_id = upload_id.to_string();
            let pb = pb.map(|pb| pb.clone());

            let permit = Arc::clone(&sem).acquire_owned().await;
            join_set.spawn(async move {
                let _permit = permit; // consume semaphore

                let this_chunk = if chunk_count - 1 == chunk_index {
                    size_of_last_chunk
                } else {
                    multipart_chunk_size
                };

                let (part_number, upload_part_output) = RetryIf::spawn(
                    retry_strategy
                        .clone()
                        .map(jitter)
                        .take(max_retries),
                    || async {

                    let body = ByteStream::read_from()
                        .path(&local_path)
                        .buffer_size(UPLOAD_BYTESTREAM_BUFFER_SIZE)
                        .offset(chunk_index * MULTIPART_CHUNK_SIZE)
                        .length(Length::Exact(this_chunk))
                        .build()
                        .await.map_err(partial!(map_bytestream_error => format!("<upload_object_multipart> bucket={bucket} key={key} upload_chunk_index={chunk_index}"), _))?;

                    // Chunk index needs to start at 0, but part numbers start at 1.
                    let part_number = (chunk_index as i32) + 1;
                    let upload_part_output = client
                        .upload_part()
                        .bucket(&bucket)
                        .key(&key)
                        .upload_id(&upload_id)
                        .body(body)
                        .part_number(part_number)
                        .send()
                        .await
                        .map_err(
                            partial!(map_sdk_error => format!("<upload_object_multipart> bucket={bucket} key={key} upload_chunk_index={chunk_index}"), _),
                        )?;
                        anyhow::Ok((part_number, upload_part_output))
                    },
                    should_retry,
                )
                .await?;

                pb.map(|pb| {
                    pb.inc(1);
                });
                anyhow::Ok((
                    upload_part_output.e_tag.context("No etag!")?,
                    part_number,
                ))
            });
        }

        // collect results
        let mut upload_parts: Vec<CompletedPart> = Vec::new();
        let mut res_ = Ok(());
        while let Some(res) = join_set.join_next().await {
            if let Ok(res) = res {
                match res {
                    Ok((e_tag, part_number)) => {
                        upload_parts.push(
                            CompletedPart::builder()
                                .e_tag(e_tag)
                                .part_number(part_number)
                                .build(),
                        );
                    }
                    Err(e) => {
                        // abort all tasks and break on first error encountered
                        join_set.abort_all();
                        res_ = Err(e);
                        break;
                    }
                }
            } else {
                // canceled
                res_ = Err(anyhow!("Multipart upload of {local_path} was canceled!"));
                break;
            }
        }

        // evaluate errors and send abort multipart upload request if error
        let abort = || async {
            let _ = self.client
                .abort_multipart_upload()
                .bucket(bucket)
                .key(key)
                .upload_id(upload_id)
                .send()
                .await
                .map_err(partial!(map_sdk_error => format!("<upload_object_multipart> bucket={bucket} key={key}"), _));
        };

        // in case of any error with early loop break
        if let Err(e) = res_ {
            error!("<upload_object_multipart> bucket={bucket} key={key} Failed to upload all parts! Abort multipart upload.");
            abort().await;
            return Err(e);
        }

        // if not it would indicate logical error
        if upload_parts.len() != chunk_count as usize {
            error!("<upload_object_multipart> bucket={bucket} key={key} Chunk count not lined up! Abort multipart upload.");
            abort().await;
            bail!("Failed to upload all parts!");
        }

        // sort by part number
        upload_parts.sort_by(|a, b| a.part_number.cmp(&b.part_number));

        // complete multipart upload
        let client = self.client.clone();
        let upload_parts_ref = &upload_parts;
        let complete_multipart_upload_res = RetryIf::spawn(
            self.retry_strategy
                .clone()
                .map(jitter)
                .take(self.max_retries),
            || async {
                let complete_multipart_upload_output = client
                    .complete_multipart_upload()
                    .bucket(bucket)
                    .key(key)
                    .multipart_upload(
                        CompletedMultipartUpload::builder()
                            .set_parts(Some(upload_parts_ref.clone()))
                            .build(),
                    )
                    .upload_id(upload_id)
                    .send()
                    .await
                    .map_err(partial!(map_sdk_error => format!("<upload_object_multipart> bucket={bucket} key={key}"), _))?;
                anyhow::Ok(complete_multipart_upload_output)
            },
            should_retry,
        ).await;

        if let Err(e) = complete_multipart_upload_res {
            error!("<upload_object_multipart> bucket={bucket} key={key} Failed to complete multipart upload! Abort multipart upload.");
            abort().await;
            return Err(e.into());
        }

        debug!(
            "Uploaded multipart from {} to s3://{}/{}",
            local_path, bucket, key
        );

        Ok(())
    }

    pub async fn delete_object(&self, bucket: &str, key: &str) -> Result<()> {
        RetryIf::spawn(
            self.retry_strategy.clone().map(jitter).take(self.max_retries),
            || async {
                self
                    .client
                    .delete_object()
                    .bucket(bucket)
                    .key(key)
                    .send()
                    .await
                    .map_err(partial!(map_sdk_error => format!("<delete_object> bucket={bucket} key={key}"), _))
                    .map_err(|e| e.into())
            },
            should_retry,
        )
        .await?;

        debug!("Deleted s3://{}/{}", bucket, key);
        Ok(())
    }

    pub async fn copy_object(
        &self,
        src_bucket: &str,
        src_key: &str,
        dest_bucket: &str,
        dest_key: &str,
        storage_class: Option<&str>,
    ) -> Result<()> {
        RetryIf::spawn(
            self.retry_strategy.clone().map(jitter).take(self.max_retries),
            || async {
                let mut builder = self.client
                    .copy_object()
                    .bucket(dest_bucket)
                    .key(dest_key)
                    .copy_source(format!("{}/{}", src_bucket, src_key));

                if let Some(storage_class) = storage_class {
                    // let storage_class = match storage_class {
                    //     "STANDARD" => StorageClass::Standard,
                    //     "REDUCED_REDUNDANCY" => StorageClass::ReducedRedundancy,
                    //     "STANDARD_IA" => StorageClass::StandardIa,
                    //     "ONEZONE_IA" => StorageClass::OnezoneIa,
                    //     "INTELLIGENT_TIERING" => StorageClass::IntelligentTiering,
                    //     "GLACIER" => StorageClass::Glacier,
                    //     "DEEP_ARCHIVE" => StorageClass::DeepArchive,
                    //     "OUTPOSTS" => StorageClass::Outposts,
                    //     "GLACIER_IR" => StorageClass::GlacierIr,
                    //     "SNOW" => StorageClass::Snow,
                    //     "EXPRESS_ONEZONE" => StorageClass::ExpressOnezone,
                    //     _ => {
                    //         bail!("Invalid storage class: {}", storage_class);
                    //     }
                    // };
                    let storage_class = StorageClass::from(storage_class);
                    builder = builder.storage_class(storage_class);
                }

                builder
                    .send()
                    .await
                    .map_err(partial!(map_sdk_error => format!("<copy_object> src_bucket={src_bucket} src_key={src_key} dest_bucket={dest_bucket} dest_key={dest_key} storage_class={:?}", storage_class), _))
                    .map_err(|e| e.into())
            },
            should_retry,
        )
        .await?;

        debug!(
            "Copied s3://{}/{} to s3://{}/{} (storage_class={:?})",
            src_bucket, src_key, dest_bucket, dest_key, storage_class
        );

        Ok(())
    }

    pub async fn restore_object(
        &self,
        bucket: &str,
        key: &str,
        days: i32,
        tier: &str,
    ) -> Result<()> {
        RetryIf::spawn(
            self.retry_strategy.clone().map(jitter).take(self.max_retries),
            || async {
                let restore_request = RestoreRequest::builder().days(days).glacier_job_parameters(
                    GlacierJobParameters::builder().tier(Tier::from(tier)).build()?
                ).build();
                self.client
                    .restore_object()
                    .bucket(bucket)
                    .key(key)
                    .restore_request(restore_request)
                    .send()
                    .await
                    .map_err(partial!(map_sdk_error => format!("<restore_object> bucket={bucket} key={key} days={days} tier={:?}", tier), _))
                    .map_err(|e| e.into())
            },
            should_retry,
        )
        .await?;

        debug!(
            "Restored s3://{}/{} (days={}, tier={})",
            bucket, key, days, tier
        );

        Ok(())
    }
}
