use crate::retry_strategy::RetryStrategy;
use crate::utils::generate_random_hex;
use aws_config::Region;
use aws_credential_types::Credentials;
use aws_sdk_s3::{
    error::{ErrorMetadata, ProvideErrorMetadata, SdkError},
    operation::list_objects_v2::ListObjectsV2Output,
    primitives::{ByteStream, ByteStreamError, DateTime, Length},
    types::{
        CompletedMultipartUpload, CompletedPart, GlacierJobParameters, RestoreRequest,
        StorageClass, Tier,
    },
    Client as AWSS3Client, Error as AWSS3Error,
};
use bytes::Bytes;
use core::str;
#[allow(unused_imports)]
use log::{debug, error, info, trace, warn};
use partial_application::partial;
use std::io::{BufWriter, Seek, Write};
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot, Semaphore};
use tokio_retry::RetryIf;

/// Default read timeout in seconds for the underlying HTTP client (boto default).
pub const DEFAULT_READ_TIMEOUT: u64 = 60;
/// Default maximum number of attempts for our high-level retries (0 means no high-level retries and rely on the underlying SDK retries).
pub const DEFAULT_MAX_RETRIES: usize = 0;
/// Default HTTP status codes treated as retriable client errors (408 Request Timeout, 429 Too Many Requests).
/// Error code SlowDown is also retried.
pub const DEFAULT_RETRIABLE_CLIENT_STATUS_CODES: &[u16] = &[408, 429];
/// Comma-separated default for CLI; must match DEFAULT_RETRIABLE_CLIENT_STATUS_CODES.
pub const DEFAULT_RETRIABLE_CLIENT_STATUS_CODES_STR: &str = "408,429";
/// Region used when the environment/config does not provide one.
pub const FALLBACK_REGION: &str = "us-east-1";
/// Buffer size for upload byte stream (1 MiB).
pub const FILE_BUFFER_SIZE: usize = 1024 * 1024;
/// Part size for multipart upload/download (20 MiB). Adaptive when file size would exceed MULTIPART_MAX_CHUNKS parts.
pub const DEFAULT_MULTIPART_CHUNK_SIZE: u64 = 1024 * 1024 * 20;
/// Number of parallel workers for multipart download/upload when not overridden per call (default: 1).
pub const DEFAULT_MULTIPART_N_WORKERS: usize = 1;
/// S3 API limit on number of parts per multipart upload (10_000).
pub const MULTIPART_MAX_CHUNKS: u64 = 10000;

/// Progress bar for multipart transfer (e.g. chunk count). Use with [`download_object_multipart`](S3Client::download_object_multipart) and [`upload_object_multipart`](S3Client::upload_object_multipart).
/// When the `indicatif` feature is enabled, [`indicatif::ProgressBar`] implements this trait.
pub trait ProgressBar: Send + Sync + Clone {
    /// Set the total number of units (e.g. chunks).
    fn set_length(&self, len: u64);
    /// Advance by `delta` units (e.g. one chunk completed).
    fn inc(&self, delta: u64);
    /// Mark the progress bar as finished.
    fn finish(&self);
}

#[cfg(feature = "indicatif")]
impl ProgressBar for indicatif::ProgressBar {
    fn set_length(&self, len: u64) {
        indicatif::ProgressBar::set_length(self, len);
    }
    fn inc(&self, delta: u64) {
        indicatif::ProgressBar::inc(self, delta);
    }
    fn finish(&self) {
        indicatif::ProgressBar::finish(self);
    }
}

/// No-op progress bar. Use when progress reporting is not needed (e.g. in tests).
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopProgressBar;

impl ProgressBar for NoopProgressBar {
    fn set_length(&self, _len: u64) {}
    fn inc(&self, _delta: u64) {}
    fn finish(&self) {}
}

/// Configuration for the underlying AWS S3 client (region, endpoint, credentials, timeouts).
#[derive(Clone, Debug)]
pub struct S3ClientConfig {
    pub region: Option<String>,
    pub endpoint_url: Option<String>,
    pub profile_name: Option<String>,
    pub access_secret_session_tuple: Option<(String, String, Option<String>)>,
    /// Read timeout in seconds for the HTTP client (default: 60).
    pub read_timeout_secs: u64,
    /// Part size for multipart upload/download in bytes (default: 20 MiB). Adaptive when file size would exceed MULTIPART_MAX_CHUNKS parts.
    pub multipart_chunk_size: u64,
    /// Number of parallel workers for multipart download/upload when not overridden per call (default: 1).
    pub multipart_n_workers: usize,
}

impl Default for S3ClientConfig {
    fn default() -> Self {
        Self {
            region: None,
            endpoint_url: None,
            profile_name: None,
            access_secret_session_tuple: None,
            read_timeout_secs: DEFAULT_READ_TIMEOUT,
            multipart_chunk_size: DEFAULT_MULTIPART_CHUNK_SIZE,
            multipart_n_workers: DEFAULT_MULTIPART_N_WORKERS,
        }
    }
}

/// Configuration for retry behavior (max retries, strategy, and which client status codes to retry).
/// Use [`RetryConfig::default`] for default retry behavior (no high-level retries).
#[derive(Clone, Debug)]
pub struct RetryConfig {
    pub max_retries: usize,
    pub retry_strategy: RetryStrategy,
    pub retriable_client_status_codes: Vec<u16>,
}

/// Default retry configuration (max_retries=0, exponential backoff strategy, retriable client status codes: 408, 429).
impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: DEFAULT_MAX_RETRIES,
            retry_strategy: RetryStrategy::default(),
            retriable_client_status_codes: DEFAULT_RETRIABLE_CLIENT_STATUS_CODES.to_vec(),
        }
    }
}

/// BucketInfo
#[derive(Clone, Debug)]
pub struct BucketInfo {
    pub name: String,
    pub region: Option<String>,
}

/// Metadata for an S3 object (from HEAD or LIST).
#[derive(Clone, Debug)]
pub struct ObjectInfo {
    pub key: String,
    pub size: u64,
    /// Last-modified time (AWS SDK `DateTime`).
    pub timestamp: DateTime,
    pub storage_class: Option<String>,
    pub restore_status: Option<String>,
}

/// A common prefix from a list_objects_v2 response (delimiter-based "directory").
#[derive(Clone, Debug)]
pub struct CommonPrefixInfo {
    pub prefix: String,
}

/// Page-by-page iterator for list_objects_v2. Yields one page at a time; retries are applied
/// per page request, so a failure on one page does not invalidate the iterator.
/// MaxKeys is not set (SDK default, typically 1000 keys per page).
///
/// Uses `Option<Option<String>>` for continuation: `None` = first request not yet made,
/// `Some(None)` = no more pages, `Some(Some(token))` = use token for next request.
pub struct ListObjectsV2PageIter<'a> {
    s3_client: &'a S3Client,
    bucket: &'a str,
    prefix: &'a str,
    delimiter: Option<&'a str>,
    /// None = first page not fetched; Some(None) = exhausted; Some(Some(t)) = next token
    continuation_token: Option<Option<String>>,
}

impl<'a> ListObjectsV2PageIter<'a> {
    /// Fetches the next page. Returns `Ok(None)` when there are no more pages.
    /// Retries (using the client's retry config) are applied to this single page request only.
    pub async fn next_page(&mut self) -> Result<Option<(Vec<ObjectInfo>, Vec<CommonPrefixInfo>)>> {
        if let Some(None) = self.continuation_token {
            return Ok(None);
        }

        let s3_client = self.s3_client;
        let resp = s3_client
            .with_retry(|| async {
                let mut builder = s3_client
                    .inner
                    .list_objects_v2()
                    .bucket(self.bucket)
                    .prefix(self.prefix);
                if let Some(d) = self.delimiter {
                    builder = builder.delimiter(d);
                }
                if let Some(Some(t)) = &self.continuation_token {
                    builder = builder.continuation_token(t);
                }
                builder.send().await.map_err(|e| {
                    map_sdk_error(
                        format!(
                            "<list_objects_v2_paginate_pages> bucket={} prefix={}",
                            self.bucket, self.prefix
                        ),
                        s3_client
                            .retry_config
                            .retriable_client_status_codes
                            .as_slice(),
                        e,
                    )
                })
            })
            .await?;

        let more = resp.is_truncated() == Some(true)
            && resp
                .next_continuation_token()
                .map(|s| !s.is_empty())
                .unwrap_or(false);
        self.continuation_token = Some(if more {
            resp.next_continuation_token().map(String::from)
        } else {
            None
        });

        Ok(Some(page_to_object_and_prefix_lists(&resp)?))
    }
}

/// Converts one SDK list_objects_v2 page into `(Vec<ObjectInfo>, Vec<CommonPrefixInfo>)`.
#[allow(clippy::result_large_err)]
fn page_to_object_and_prefix_lists(
    item: &ListObjectsV2Output,
) -> Result<(Vec<ObjectInfo>, Vec<CommonPrefixInfo>)> {
    let mut objects: Vec<ObjectInfo> = vec![];
    let mut common_prefixes: Vec<CommonPrefixInfo> = vec![];
    item.contents().iter().try_for_each(|object| {
        objects.push(ObjectInfo {
            key: object.key().ok_or(S3Error::FieldNotExist("key"))?.into(),
            size: object.size().ok_or(S3Error::FieldNotExist("size"))? as u64,
            timestamp: object
                .last_modified()
                .ok_or(S3Error::FieldNotExist("timestamp"))?
                .to_owned(),
            storage_class: object.storage_class().map(|sc| sc.as_str().to_owned()),
            restore_status: object.restore_status().map(|rs| {
                let is_restoring = if let Some(is_restoring) = rs.is_restore_in_progress {
                    if is_restoring {
                        "Restoring"
                    } else {
                        "Restored"
                    }
                } else {
                    "N/A"
                };
                let ts = rs.restore_expiry_date().map(|red| red.to_string());
                format!(
                    "{} (Expires: {})",
                    is_restoring,
                    ts.unwrap_or("N/A".to_owned())
                )
            }),
        });
        Result::Ok(())
    })?;
    item.common_prefixes()
        .iter()
        .try_for_each(|common_prefix| {
            common_prefixes.push(CommonPrefixInfo {
                prefix: common_prefix
                    .prefix()
                    .ok_or(S3Error::FieldNotExist("prefix"))?
                    .into(),
            });
            Result::Ok(())
        })?;
    Ok((objects, common_prefixes))
}

#[derive(Error, Debug)]
pub enum S3Error {
    #[error("{} [ConstructionFailure]", .0)]
    ConstructionFailure(String),
    #[error("{} [TimeoutError]", .0)]
    TimeoutError(String),
    #[error("{} [DispatchFailure]", .0)]
    DispatchFailure(String),
    #[error("{} [ResponseError]", .0)]
    ResponseError(String),
    #[error("{} [RetriableClientError - <{}> <{}> <{}>]", .0, .1, .2, .3)]
    RetriableClientError(String, AWSS3Error, ErrorMetadata, u16),
    #[error("{} [RetriableServerError - <{}> <{}> <{}>]", .0, .1, .2, .3)]
    RetriableServerError(String, AWSS3Error, ErrorMetadata, u16),
    #[error("{} [AWSS3Error - <{}> <{}> <{}>]", .0, .1, .2, .3)]
    AWSS3Error(String, AWSS3Error, ErrorMetadata, u16),
    #[error("{} [OtherSDKError - <{}>]", .0, .1)]
    OtherSDKError(String, AWSS3Error),
    #[error("{} [ByteStreamDownloadError - <{}>]", .0, .1)]
    ByteStreamDownloadError(String, ByteStreamError),
    #[error("{} [ByteStreamUploadError - <{}>]", .0, .1)]
    ByteStreamUploadError(String, ByteStreamError),
    #[error("{} [ValidationError]", .0)]
    ValidationError(String),
    #[error("{} [IOError]", .0)]
    IOError(String),
    #[error("{} [FieldNotExist]", .0)]
    FieldNotExist(&'static str),
    #[error("{} [RuntimeError]", .0)]
    RuntimeError(String),
}

impl From<std::io::Error> for S3Error {
    fn from(e: std::io::Error) -> Self {
        S3Error::IOError(e.to_string())
    }
}

fn map_sdk_error<E>(
    context: String,
    retriable_client_status_codes: &[u16],
    e: SdkError<E>,
) -> S3Error
where
    AWSS3Error: From<SdkError<E>>,
    E: ProvideErrorMetadata + std::fmt::Debug,
{
    match &e {
        SdkError::ConstructionFailure(construction_error) => {
            debug!("[ConstructionFailure] {:?}", construction_error);
            S3Error::ConstructionFailure(context)
        }
        SdkError::TimeoutError(timeout_error) => {
            debug!("[TimeoutError] {:?}", timeout_error);
            S3Error::TimeoutError(context)
        }
        SdkError::DispatchFailure(dispatch_error) => {
            debug!(
                "[DispatchFailure] is_io: {} is_timeout: {} is_user: {} is_other: {} {:?}",
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
                if let Ok(raw_content) = str::from_utf8(bytes) {
                    if !raw_content.is_empty() {
                        debug!("[ResponseError] raw {}", raw_content);
                    }
                }
            }
            S3Error::ResponseError(context)
        }
        SdkError::ServiceError(service_error) => {
            if let Some(bytes) = service_error.raw().body().bytes() {
                if let Ok(raw_content) = str::from_utf8(bytes) {
                    if !raw_content.is_empty() {
                        debug!("[ServiceError] raw {}", raw_content);
                    }
                }
            }

            let error_meta = e.meta().to_owned();
            debug!("[ServiceError] error_meta {:?}", error_meta);

            let status_code = service_error.raw().status().as_u16();
            debug!("[ServiceError] status_code {}", status_code);

            if retriable_client_status_codes.contains(&status_code)
                || error_meta.code() == Some("SlowDown")
            {
                S3Error::RetriableClientError(context, e.into(), error_meta, status_code)
            } else if status_code >= 500 {
                S3Error::RetriableServerError(context, e.into(), error_meta, status_code)
            } else {
                S3Error::AWSS3Error(context, e.into(), error_meta, status_code)
            }
        }
        _ => {
            error!("{context} {:?}", e);
            S3Error::OtherSDKError(context, e.into())
        }
    }
}

fn map_bytestream_download_error(context: String, e: ByteStreamError) -> S3Error {
    debug!("{context} {:?}", e);
    S3Error::ByteStreamDownloadError(context, e)
}

fn map_bytestream_upload_error(context: String, e: ByteStreamError) -> S3Error {
    debug!("{context} {:?}", e);
    S3Error::ByteStreamUploadError(context, e)
}

fn should_retry(e: &S3Error) -> bool {
    match e {
        S3Error::TimeoutError(_)
        | S3Error::DispatchFailure(_)
        | S3Error::ResponseError(_)
        | S3Error::RetriableClientError(_, _, _, _)
        | S3Error::RetriableServerError(_, _, _, _)
        | S3Error::ByteStreamDownloadError(_, _) => {
            info!("RetryIf: {}. Retrying...", e);
            true
        }
        _ => {
            // other S3Error errors
            debug!("RetryIf: {}. Not retrying...", e);
            false
        }
    }
}

pub type Result<T> = std::result::Result<T, S3Error>;

#[derive(Debug, Clone)]
pub struct S3Client {
    pub inner: AWSS3Client,
    retry_config: RetryConfig,
    multipart_chunk_size: u64,
    multipart_n_workers: usize,
}

impl S3Client {
    /// Build an S3 client. Use [`RetryConfig::default`] for default AWS client retry behavior (no high-level retries from this crate).
    /// When both high-level (this crate) and low-level (SDK) retries are enabled, logs a warning (double retries).
    pub async fn new(config: S3ClientConfig, retry_config: RetryConfig) -> Self {
        let mut config_loader = aws_config::from_env();
        if let Some(region) = &config.region {
            config_loader = config_loader.region(Region::new(region.clone()))
        }

        if let Some(endpoint_url) = &config.endpoint_url {
            config_loader = config_loader.endpoint_url(endpoint_url);
        }

        if let Some(profile_name) = &config.profile_name {
            config_loader = config_loader.profile_name(profile_name);
        }
        if let Some((access_key, secret_key, session_token)) = &config.access_secret_session_tuple {
            config_loader = config_loader.credentials_provider(Credentials::from_keys(
                access_key.clone(),
                secret_key.clone(),
                session_token.clone(),
            ));
        }

        config_loader = config_loader.timeout_config(
            aws_config::timeout::TimeoutConfig::builder()
                .read_timeout(Duration::from_secs(config.read_timeout_secs))
                .build(),
        );

        // if enrolling into high-level retries, disable low-level retries
        if retry_config.max_retries > 0 {
            config_loader = config_loader.retry_config(aws_config::retry::RetryConfig::disabled())
        }

        let sdk_config = config_loader.load().await;
        let mut config_builder = aws_sdk_s3::config::Builder::from(&sdk_config);
        if sdk_config.region().is_none() {
            info!(
                "Can't resolve region. Using fallback region: {}",
                FALLBACK_REGION
            );
            config_builder = config_builder.region(Region::new(FALLBACK_REGION));
        }
        config_builder = config_builder.force_path_style(true); // this allows http://minio:11000 style endpoint_url

        S3Client {
            inner: AWSS3Client::from_conf(config_builder.build()),
            retry_config,
            multipart_chunk_size: config.multipart_chunk_size,
            multipart_n_workers: config.multipart_n_workers,
        }
    }

    /// Build from an existing SDK client. Use [`RetryConfig::default`] for default AWS client retry behavior (no high-level retries from this crate).
    /// When both high-level (this crate) and low-level (SDK) retries are enabled, logs a warning (double retries).
    /// Uses [`DEFAULT_MULTIPART_CHUNK_SIZE`] and [`DEFAULT_MULTIPART_N_WORKERS`] unless overridden.
    pub fn new_with_aws_s3_client(
        aws_s3_client: AWSS3Client,
        retry_config: RetryConfig,
        multipart_chunk_size: Option<u64>,
        multipart_n_workers: Option<usize>,
    ) -> Self {
        if retry_config.max_retries > 0 && aws_s3_client.config().retry_config().is_some() {
            warn!("High-level retries are enabled but low-level retries are also enabled.");
        }

        S3Client {
            inner: aws_s3_client,
            retry_config,
            multipart_chunk_size: multipart_chunk_size.unwrap_or(DEFAULT_MULTIPART_CHUNK_SIZE),
            multipart_n_workers: multipart_n_workers.unwrap_or(DEFAULT_MULTIPART_N_WORKERS),
        }
    }

    /// Runs an operation with the client's retry policy (strategy + max_retries + should_retry).
    async fn with_retry<F, Fut, T>(&self, op: F) -> Result<T>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        RetryIf::spawn(
            self.retry_config
                .retry_strategy
                .clone()
                .delay_iterator_with_jitter(self.retry_config.max_retries),
            op,
            should_retry,
        )
        .await
    }

    pub async fn head_bucket(&self, bucket: &str) -> Result<BucketInfo> {
        self.inner
            .head_bucket()
            .bucket(bucket)
            .send()
            .await
            .map_err(partial!(map_sdk_error => format!("<head_bucket> bucket={bucket}"), self.retry_config.retriable_client_status_codes.as_slice(), _))?;
        Ok(BucketInfo {
            name: bucket.into(),
            region: self.inner.config().region().map(|r| r.to_string()),
        })
    }

    pub async fn create_bucket(&self, bucket: &str) -> Result<()> {
        self.inner
            .create_bucket()
            .bucket(bucket)
            .send()
            .await
            .map_err(partial!(
                map_sdk_error => format!("<create_bucket> bucket={bucket}"),
                self.retry_config.retriable_client_status_codes.as_slice(),
                _
            ))?;
        Ok(())
    }

    pub async fn delete_bucket(&self, bucket: &str) -> Result<()> {
        self.inner
            .delete_bucket()
            .bucket(bucket)
            .send()
            .await
            .map_err(partial!(map_sdk_error => format!("<delete_bucket> bucket={bucket}"), self.retry_config.retriable_client_status_codes.as_slice(), _))?;
        Ok(())
    }

    async fn _list_objects_v2_paginated(
        &self,
        bucket: &str,
        prefix: &str,
        delimiter: Option<&str>,
    ) -> Result<(Vec<ObjectInfo>, Vec<CommonPrefixInfo>)> {
        let mut builder = self.inner.list_objects_v2().bucket(bucket).prefix(prefix);

        if let Some(delimiter) = delimiter {
            builder = builder.delimiter(delimiter);
        }

        let mut pagination_stream = builder.into_paginator().send();

        let mut objects: Vec<ObjectInfo> = vec![];
        let mut common_prefixes: Vec<CommonPrefixInfo> = vec![];
        while let Some(item) = pagination_stream
            .try_next()
            .await
            .map_err(partial!(map_sdk_error => format!("<list_objects_v2_paginated> bucket={bucket} prefix={prefix}"), self.retry_config.retriable_client_status_codes.as_slice(), _))?
        {
            let (mut objs, mut prefixes) = page_to_object_and_prefix_lists(&item)?;
            objects.append(&mut objs);
            common_prefixes.append(&mut prefixes);
        }
        Ok((objects, common_prefixes))
    }

    pub async fn list_objects_v2_paginated(
        &self,
        bucket: &str,
        prefix: &str,
        delimiter: Option<&str>,
    ) -> Result<(Vec<ObjectInfo>, Vec<CommonPrefixInfo>)> {
        let (objects, common_prefixes) = self
            .with_retry(|| async {
                self._list_objects_v2_paginated(bucket, prefix, delimiter)
                    .await
            })
            .await?;

        debug!(
            "Prefix {}: Found {} objects and {} common prefixes.",
            prefix,
            objects.len(),
            common_prefixes.len()
        );

        Ok((objects, common_prefixes))
    }

    /// Returns an iterator that yields one list_objects_v2 page at a time. Retries are applied
    /// per page request (each call to `next_page()`), so the iterator is not invalidated by
    /// a transient failure on one page.
    pub fn list_objects_v2_page_iter<'a>(
        &'a self,
        bucket: &'a str,
        prefix: &'a str,
        delimiter: Option<&'a str>,
    ) -> ListObjectsV2PageIter<'a> {
        ListObjectsV2PageIter {
            s3_client: self,
            bucket,
            prefix,
            delimiter,
            continuation_token: None,
        }
    }

    pub async fn head_object(&self, bucket: &str, key: &str) -> Result<ObjectInfo> {
        let resp = self
            .with_retry(|| async {
                self.inner
                    .head_object()
                    .bucket(bucket)
                    .key(key)
                    .send()
                    .await
                    .map_err(partial!(map_sdk_error => format!("<head_object> bucket={bucket} key={key}"), self.retry_config.retriable_client_status_codes.as_slice(), _))
            })
            .await?;

        let object_info = ObjectInfo {
            key: key.into(),
            size: resp
                .content_length()
                .ok_or(S3Error::FieldNotExist("size"))? as u64,
            timestamp: resp
                .last_modified()
                .ok_or(S3Error::FieldNotExist("timestamp"))?
                .to_owned(),
            storage_class: resp.storage_class().map(|sc| sc.as_str().to_owned()),
            restore_status: resp.restore().map(|rs| rs.to_owned()),
        };
        debug!("Found object with key={}", key);
        debug!("Content length: {}", object_info.size);
        debug!("Last modified: {}", object_info.timestamp);
        debug!("Storage class: {:?}", object_info.storage_class);
        debug!(
            "Content type: {}",
            resp.content_type()
                .ok_or(S3Error::FieldNotExist("content_type"))?
        );

        Ok(object_info)
    }

    async fn _get_object(
        &self,
        bucket: &str,
        key: &str,
        start_end_offsets: Option<(usize, usize)>,
    ) -> Result<(ObjectInfo, Vec<u8>)> {
        let mut builder = self.inner.get_object().bucket(bucket).key(key);

        if let Some(start_end_offsets) = start_end_offsets {
            if start_end_offsets.1 <= start_end_offsets.0 {
                return Err(S3Error::ValidationError(format!(
                    "Invalid start_end_offsets, non-positive slice: start {} end {}!",
                    start_end_offsets.0, start_end_offsets.1
                )));
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
            partial!(map_sdk_error => format!("<get_object> bucket={bucket} key={key}"), self.retry_config.retriable_client_status_codes.as_slice(), _),
        )?;

        let object_info = ObjectInfo {
            key: key.into(),
            size: resp
                .content_length()
                .ok_or(S3Error::FieldNotExist("size"))? as u64,
            timestamp: resp
                .last_modified()
                .ok_or(S3Error::FieldNotExist("timestamp"))?
                .to_owned(),
            storage_class: resp.storage_class().map(|sc| sc.as_str().to_owned()),
            restore_status: resp.restore().map(|rs| rs.to_owned()),
        };

        debug!("Found object with key={}", key);
        debug!("Content length: {}", object_info.size);
        debug!("Last modified: {}", object_info.timestamp);
        debug!("Storage class: {:?}", object_info.storage_class);
        debug!(
            "Content type: {}",
            resp.content_type()
                .ok_or(S3Error::FieldNotExist("content_type"))?
        );

        let content = resp
            .body
            .collect()
            .await
            .map_err(partial!(map_bytestream_download_error => format!("<get_object> bucket={bucket} key={key}"), _))?
            .into_bytes().to_vec();
        Ok((object_info, content))
    }

    pub async fn get_object(
        &self,
        bucket: &str,
        key: &str,
        start_end_offsets: Option<(usize, usize)>,
    ) -> Result<(ObjectInfo, Vec<u8>)> {
        let (object_info, content) = self
            .with_retry(|| async { self._get_object(bucket, key, start_end_offsets).await })
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
        let mut builder = self.inner.get_object().bucket(bucket).key(key);

        if let Some(start_end_offsets) = start_end_offsets {
            if start_end_offsets.1 <= start_end_offsets.0 {
                return Err(S3Error::ValidationError(format!(
                    "Invalid start_end_offsets, non-positive slice: start {} end {}!",
                    start_end_offsets.0, start_end_offsets.1
                )));
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
            partial!(map_sdk_error => format!("<download_object> bucket={bucket} key={key}"), self.retry_config.retriable_client_status_codes.as_slice(), _),
        )?;

        let object_info = ObjectInfo {
            key: key.into(),
            size: resp
                .content_length()
                .ok_or(S3Error::FieldNotExist("size"))? as u64,
            timestamp: resp
                .last_modified()
                .ok_or(S3Error::FieldNotExist("timestamp"))?
                .to_owned(),
            storage_class: resp.storage_class().map(|sc| sc.as_str().to_owned()),
            restore_status: resp.restore().map(|rs| rs.to_owned()),
        };

        let local_path = local_path.to_owned();
        let timestamp = object_info.timestamp;

        // We create a temporary file to download the object to for atomicity.
        // Temp file is cleaned up on any error (stream, write, flush, mtime, or rename).
        let random_suffix = generate_random_hex(8);
        let local_path_tmp = format!("{local_path}.{random_suffix}");

        let (sender, mut receiver) = mpsc::channel::<Vec<u8>>(256);
        let (tx, rx) = oneshot::channel::<()>();
        let local_path_tmp_w = local_path_tmp.clone();
        let local_path_w = local_path.clone();
        // Use spawn_blocking to avoid slowing down the async runtime.
        let t_writer = tokio::task::spawn_blocking(move || {
            let res = (|| {
                let mut file = BufWriter::with_capacity(
                    FILE_BUFFER_SIZE, // 1MB buffer
                    std::fs::File::create(&local_path_tmp_w)?,
                );
                while let Some(bytes) = receiver.blocking_recv() {
                    file.write_all(&bytes)?;
                }
                file.flush()?;
                // Success indicator: if Ok, parent completed the stream; set mtime and rename.
                if rx.blocking_recv().is_ok() {
                    // Set the file mtime according to object timestamp.
                    filetime::set_file_mtime(
                        &local_path_tmp_w,
                        filetime::FileTime::from_unix_time(
                            timestamp.secs(),
                            timestamp.subsec_nanos(),
                        ),
                    )?;
                    std::fs::rename(&local_path_tmp_w, &local_path_w)?;
                    return Ok(true);
                }
                Result::Ok(false)
            })();
            match res {
                Ok(true) => Result::Ok(()),
                Ok(false) => {
                    // Parent failed (stream error or sender dropped). Parent returns its own error (may be retried).
                    // Small chance this task is canceled when runtime is shutting down; ok because we use random-suffixed temp files.
                    // We don't bother returning the error from this task.
                    let _ = std::fs::remove_file(&local_path_tmp_w);
                    Result::Ok(())
                }
                Err(e) => {
                    // On writer task failures, cleanup temp file and propagate the error.
                    let _ = std::fs::remove_file(&local_path_tmp_w);
                    Err(e)
                }
            }
        });

        let stream_result: Result<()> = async {
            while let Some(bytes) = resp
                .body
                .try_next()
                .await
                .map_err(partial!(
                    map_bytestream_download_error => format!("<download_object> bucket={bucket} key={key}"), _
                ))?
            {
                if sender.send(bytes.to_vec()).await.is_err() {
                    break;
                }
            }

            Ok(())
        }
        .await;

        // Dropping sender exits the writer's while loop.
        drop(sender);
        match stream_result {
            Ok(()) => {
                // Signal success; catch send error by awaiting the task below.
                let _ = tx.send(());
                // Writer errors (e.g. IO) are not retriable.
                t_writer
                    .await
                    .map_err(|e| S3Error::RuntimeError(e.to_string()))??;
                Ok(object_info)
            }
            Err(e) => {
                drop(tx);
                // Await writer so it can cleanup temp file before retry.
                let _ = t_writer.await;
                Err(e)
            }
        }
    }

    pub async fn download_object(
        &self,
        bucket: &str,
        key: &str,
        local_path: &str,
        start_end_offsets: Option<(usize, usize)>,
    ) -> Result<ObjectInfo> {
        let obj = self
            .with_retry(|| async {
                self._download_object(bucket, key, local_path, start_end_offsets)
                    .await
            })
            .await?;

        trace!("Downloaded from s3://{}/{} to {}", bucket, key, local_path);

        Ok(obj)
    }

    pub async fn download_object_multipart<P>(
        &self,
        bucket: &str,
        key: &str,
        local_path: &str,
        pb: Option<&P>,
    ) -> Result<ObjectInfo>
    where
        P: ProgressBar + 'static,
    {
        let resp = self
            .with_retry(|| async {
                self.inner
                    .head_object()
                    .bucket(bucket)
                    .key(key)
                    .send()
                    .await
                    .map_err(partial!(map_sdk_error => format!("<download_object_multipart> bucket={bucket} key={key}"), self.retry_config.retriable_client_status_codes.as_slice(), _))
            })
            .await?;

        let file_size = resp
            .content_length()
            .ok_or(S3Error::FieldNotExist("size"))? as u64;
        let timestamp = resp
            .last_modified()
            .ok_or(S3Error::FieldNotExist("timestamp"))?
            .to_owned();

        let object_info = ObjectInfo {
            key: key.into(),
            size: file_size,
            timestamp: timestamp.to_owned(),
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
                Result::Ok(())
            })
            .await
            .map_err(|e| S3Error::RuntimeError(e.to_string()))??;
            debug!("Created blank file at {local_path}");
            return Ok(object_info);
        }

        let chunk_size = self.multipart_chunk_size;
        let mut chunk_count = (file_size / chunk_size) + 1;
        let mut size_of_last_chunk = file_size % chunk_size;
        if size_of_last_chunk == 0 {
            size_of_last_chunk = chunk_size;
            chunk_count -= 1;
        }
        debug!("Chunk count: {}", chunk_count);
        if let Some(p) = pb {
            p.set_length(chunk_count as u64);
        }

        let random_suffix = generate_random_hex(8);
        let local_path_tmp = format!("{local_path}.{random_suffix}");
        let local_path_tmp_ = local_path_tmp.clone();
        tokio::fs::File::create(&local_path_tmp).await?;

        // parallel download
        let sem = Arc::new(Semaphore::new(self.multipart_n_workers));
        let mut join_set = tokio::task::JoinSet::new();
        for chunk_index in 0..chunk_count {
            let retry_iterator = self
                .retry_config
                .retry_strategy
                .clone()
                .delay_iterator_with_jitter(self.retry_config.max_retries);
            let retriable_client_status_codes =
                self.retry_config.retriable_client_status_codes.clone();
            let client = self.inner.clone();
            let local_path_tmp = local_path_tmp_.clone();
            let bucket = bucket.to_string();
            let key = key.to_string();
            let pb = pb.map(|p| p.clone());

            let permit = Arc::clone(&sem).acquire_owned().await;
            join_set.spawn(async move {
                let _permit = permit; // consume semaphore

                let this_chunk = if chunk_count - 1 == chunk_index {
                    size_of_last_chunk
                } else {
                    chunk_size
                };

                let start_offset = chunk_index * chunk_size;
                let end_offset = start_offset + this_chunk;

                RetryIf::spawn(
                    retry_iterator,
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
                                partial!(map_sdk_error => format!("<download_object_multipart> bucket={bucket} key={key} download_chunk_index={chunk_index}"), retriable_client_status_codes.as_slice(), _),
                            )?;
                        debug!("Done getting chunk {}", chunk_index);

                        let (sender, mut receiver) = mpsc::channel::<Vec<u8>>(256);
                        let local_path_tmp = local_path_tmp.clone();
                        // Use spawn_blocking to avoid slowing down the async runtime.
                        let t_writer = tokio::task::spawn_blocking(move || {
                            let mut file = BufWriter::with_capacity(
                                FILE_BUFFER_SIZE, // 1MB buffer
                                std::fs::OpenOptions::new()
                                    .write(true)
                                    .open(&local_path_tmp)?,
                            );
                            file.seek(std::io::SeekFrom::Start(start_offset))?;

                            debug!("Streaming chunk {} to file", chunk_index);
                            while let Some(bytes) = receiver.blocking_recv() {
                                file.write_all(&bytes)?;
                            }
                            file.flush()?;
                            debug!("Done streaming chunk {} to file", chunk_index);
                            Result::Ok(())
                        });

                        let stream_result: Result<()> = async {
                            while let Some(bytes) =
                                resp.body.try_next().await.map_err(
                                partial!(map_bytestream_download_error => format!("<download_object_multipart> bucket={bucket} key={key} download_chunk_index={chunk_index}"), _))?
                            {
                                if sender.send(bytes.to_vec()).await.is_err() {
                                    break;
                                }
                            }
                            Ok(())
                        }.await;

                        // Dropping sender exits the writer's while loop.
                        drop(sender);
                        match stream_result {
                            Ok(()) => {
                                // Writer errors (e.g. IO) are not retriable.
                                t_writer
                                    .await
                                    .map_err(|e| S3Error::RuntimeError(e.to_string()))??;
                                Ok(())
                            }
                            Err(e) => {
                                // Await writer before returning so retry doesn't race with a detached writer.
                                let _ = t_writer.await;
                                Err(e)
                            }
                        }
                    },
                    should_retry,
                )
                .await?;

                if let Some(p) = &pb {
                    p.inc(1);
                }
                Ok(())
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
                res_ = Err(S3Error::RuntimeError(format!(
                    "Multipart download of {local_path} was canceled!"
                )));
                break;
            }
        }

        if res_.is_ok() {
            let local_path_ = local_path.to_owned();
            // set the file mtime according to object timestamp
            tokio::task::spawn_blocking(move || {
                filetime::set_file_mtime(
                    &local_path_tmp,
                    filetime::FileTime::from_unix_time(timestamp.secs(), timestamp.subsec_nanos()),
                )?;
                std::fs::rename(&local_path_tmp, &local_path_)?;
                Result::Ok(())
            })
            .await
            .map_err(|e| S3Error::RuntimeError(e.to_string()))??;
            trace!(
                "Downloaded multipart from s3://{}/{} to {}",
                bucket,
                key,
                local_path
            );
        } else {
            let _ = tokio::fs::remove_file(&local_path_tmp).await;
            // Do not finalize the file.
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
        let mut builder = self.inner.put_object().bucket(bucket).key(key).body(body);

        if let Some(storage_class) = storage_class {
            builder = builder.storage_class(StorageClass::from(storage_class));
        }

        builder.send().await.map_err(
            partial!(map_sdk_error => format!("<put_object> bucket={bucket} key={key}"), self.retry_config.retriable_client_status_codes.as_slice(), _),
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
        self.with_retry(|| async {
            self._put_object(bucket, key, content.clone(), storage_class)
                .await
        })
        .await?;

        trace!("Put from memory to s3://{}/{}", bucket, key);

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
            .buffer_size(FILE_BUFFER_SIZE)
            .build()
            .await
            .map_err(
            partial!(map_bytestream_upload_error => format!("<upload_object> bucket={bucket} key={key}"), _),
        )?;
        let mut builder = self.inner.put_object().bucket(bucket).key(key).body(body);

        if let Some(storage_class) = storage_class {
            builder = builder.storage_class(StorageClass::from(storage_class));
        }

        builder.send().await.map_err(
            partial!(map_sdk_error => format!("<upload_object> bucket={bucket} key={key}"), self.retry_config.retriable_client_status_codes.as_slice(), _),
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
        self.with_retry(|| async {
            self._upload_object(bucket, key, local_path, storage_class)
                .await
        })
        .await?;

        trace!("Uploaded from {} to s3://{}/{}", local_path, bucket, key);

        Ok(())
    }

    pub async fn upload_object_multipart<P>(
        &self,
        bucket: &str,
        key: &str,
        local_path: &str,
        storage_class: Option<&str>,
        pb: Option<&P>,
    ) -> Result<()>
    where
        P: ProgressBar + 'static,
    {
        let file_size = tokio::fs::metadata(local_path).await?.len();
        if file_size == 0 {
            self.put_object(bucket, key, vec![].as_slice(), storage_class)
                .await?;
            return Ok(());
        }

        // create the multipart upload
        let create_multipart_upload_output = self
            .with_retry(|| async {
                let mut builder = self
                    .inner
                    .create_multipart_upload()
                    .bucket(bucket)
                    .key(key);

                if let Some(storage_class) = storage_class {
                    builder = builder.storage_class(StorageClass::from(storage_class));
                }

                builder
                    .send()
                    .await
                    .map_err(partial!(map_sdk_error => format!("<upload_object_multipart> bucket={bucket} key={key}"), self.retry_config.retriable_client_status_codes.as_slice(), _))
            })
            .await?;

        let upload_id = create_multipart_upload_output
            .upload_id()
            .ok_or(S3Error::FieldNotExist("upload_id"))?;

        // Prepare the file to upload - chunking scheme. Adaptive size keeps part count <= MULTIPART_MAX_CHUNKS.
        let mut multipart_chunk_size = self.multipart_chunk_size;
        if file_size > multipart_chunk_size * MULTIPART_MAX_CHUNKS {
            multipart_chunk_size = file_size / (MULTIPART_MAX_CHUNKS - 1);
            info!("File size larger than 200GB. Using adaptive chunk size {multipart_chunk_size}.");
        }

        let mut chunk_count = (file_size / multipart_chunk_size) + 1;
        let mut size_of_last_chunk = file_size % multipart_chunk_size;
        if size_of_last_chunk == 0 {
            size_of_last_chunk = multipart_chunk_size;
            chunk_count -= 1;
        }
        debug!("Chunk count: {}", chunk_count);
        if let Some(p) = pb {
            p.set_length(chunk_count as u64);
        }

        // parallel upload
        let sem = Arc::new(Semaphore::new(self.multipart_n_workers));
        let mut join_set = tokio::task::JoinSet::new();
        for chunk_index in 0..chunk_count {
            let retry_iterator = self
                .retry_config
                .retry_strategy
                .clone()
                .delay_iterator_with_jitter(self.retry_config.max_retries);
            let retriable_client_status_codes =
                self.retry_config.retriable_client_status_codes.clone();
            let client = self.inner.clone();
            let local_path = local_path.to_string();
            let bucket = bucket.to_string();
            let key = key.to_string();
            let upload_id = upload_id.to_string();
            let pb = pb.map(|p| p.clone());

            let permit = Arc::clone(&sem).acquire_owned().await;
            join_set.spawn(async move {
                let _permit = permit; // consume semaphore

                let this_chunk = if chunk_count - 1 == chunk_index {
                    size_of_last_chunk
                } else {
                    multipart_chunk_size
                };

                let (part_number, upload_part_output) = RetryIf::spawn(
                    retry_iterator,
                    || async {

                    let body = ByteStream::read_from()
                        .path(&local_path)
                        .buffer_size(FILE_BUFFER_SIZE)
                        .offset(chunk_index * multipart_chunk_size)
                        .length(Length::Exact(this_chunk))
                        .build()
                        .await.map_err(partial!(map_bytestream_upload_error => format!("<upload_object_multipart> bucket={bucket} key={key} upload_chunk_index={chunk_index}"), _))?;

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
                            partial!(map_sdk_error => format!("<upload_object_multipart> bucket={bucket} key={key} upload_chunk_index={chunk_index}"), retriable_client_status_codes.as_slice(), _),
                        )?;
                        Ok((part_number, upload_part_output))
                    },
                    should_retry,
                )
                .await?;

                if let Some(p) = &pb {
                    p.inc(1);
                }
                Ok((
                    upload_part_output.e_tag.ok_or(S3Error::FieldNotExist("etag"))?,
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
                res_ = Err(S3Error::RuntimeError(format!(
                    "Multipart upload of {local_path} was canceled!"
                )));
                break;
            }
        }

        // evaluate errors and send abort multipart upload request if error
        let abort = || async {
            let _ = self.inner
                .abort_multipart_upload()
                .bucket(bucket)
                .key(key)
                .upload_id(upload_id)
                .send()
                .await
                .map_err(partial!(map_sdk_error => format!("<upload_object_multipart> bucket={bucket} key={key}"), self.retry_config.retriable_client_status_codes.as_slice(), _));
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
            return Err(S3Error::RuntimeError(
                "Failed to upload all parts!".to_string(),
            ));
        }

        // sort by part number
        upload_parts.sort_by(|a, b| a.part_number.cmp(&b.part_number));

        // complete multipart upload
        let client = self.inner.clone();
        let upload_parts_ref = &upload_parts;
        let complete_multipart_upload_res = self
            .with_retry(|| async {
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
                    .map_err(partial!(map_sdk_error => format!("<upload_object_multipart> bucket={bucket} key={key}"), self.retry_config.retriable_client_status_codes.as_slice(), _))?;
                Ok(complete_multipart_upload_output)
            })
            .await;

        if let Err(e) = complete_multipart_upload_res {
            error!("<upload_object_multipart> bucket={bucket} key={key} Failed to complete multipart upload! Abort multipart upload.");
            abort().await;
            return Err(e);
        }

        debug!(
            "Uploaded multipart from {} to s3://{}/{}",
            local_path, bucket, key
        );

        Ok(())
    }

    pub async fn delete_object(&self, bucket: &str, key: &str) -> Result<()> {
        self.with_retry(|| async {
            self.inner
                .delete_object()
                .bucket(bucket)
                .key(key)
                .send()
                .await
                .map_err(partial!(map_sdk_error => format!("<delete_object> bucket={bucket} key={key}"), self.retry_config.retriable_client_status_codes.as_slice(), _))
        })
        .await?;

        trace!("Deleted s3://{}/{}", bucket, key);
        Ok(())
    }

    pub async fn copy_object(
        &self,
        src_bucket: &str,
        src_key: &str,
        dst_bucket: &str,
        dst_key: &str,
        storage_class: Option<&str>,
    ) -> Result<()> {
        self.with_retry(|| async {
            let mut builder = self.inner
                .copy_object()
                .bucket(dst_bucket)
                .key(dst_key)
                .copy_source(urlencoding::encode(&format!("{}/{}", src_bucket, src_key)));

            if let Some(storage_class) = storage_class {
                let storage_class = StorageClass::from(storage_class);
                builder = builder.storage_class(storage_class);
            }

            builder
                .send()
                .await
                .map_err(partial!(map_sdk_error => format!("<copy_object> src_bucket={src_bucket} src_key={src_key} dst_bucket={dst_bucket} dst_key={dst_key} storage_class={:?}", storage_class), self.retry_config.retriable_client_status_codes.as_slice(), _))
        })
        .await?;

        trace!(
            "Copied s3://{}/{} to s3://{}/{} (storage_class={:?})",
            src_bucket,
            src_key,
            dst_bucket,
            dst_key,
            storage_class
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
        self.with_retry(|| async {
            let restore_request = RestoreRequest::builder().days(days).glacier_job_parameters(
                GlacierJobParameters::builder().tier(Tier::from(tier)).build().map_err(|e| S3Error::ValidationError(e.to_string()))?
            ).build();
            self.inner
                .restore_object()
                .bucket(bucket)
                .key(key)
                .restore_request(restore_request)
                .send()
                .await
                .map_err(partial!(map_sdk_error => format!("<restore_object> bucket={bucket} key={key} days={days} tier={tier}"), self.retry_config.retriable_client_status_codes.as_slice(), _))
        })
        .await?;

        trace!(
            "Restored s3://{}/{} (days={}, tier={})",
            bucket,
            key,
            days,
            tier
        );

        Ok(())
    }
}
