#![doc = include_str!("../README.md")]

mod retry_strategy;
mod s3_client;
mod utils;

pub use retry_strategy::RetryStrategy;
pub use s3_client::{
    CommonPrefixInfo, ListObjectsV2PageIter, ObjectInfo, RetryConfig, S3Client, S3ClientConfig,
};
