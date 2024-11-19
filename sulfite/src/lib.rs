#![doc = include_str!("../README.md")]

pub mod retry_strategy;
mod s3_client;
mod utils;

pub use s3_client::{CommonPrefixInfo, ObjectInfo, S3Client};
