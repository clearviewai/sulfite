# sulfite

[![Crates.io](https://img.shields.io/crates/v/sulfite)](https://crates.io/crates/sulfite)
[![Docs.rs](https://docs.rs/sulfite/badge.svg)](https://docs.rs/sulfite)
[![License](https://img.shields.io/crates/l/sulfite)](#license)

## Overview

`sulfite` is a high-level S3 client built on [AWS SDK for Rust](https://awslabs.github.io/aws-sdk-rust/) for even better ease of use, reliability, and bandwidth saturation (>50 Gbps).

*The name*: `SO3^2-`, an anion, implying a companion to some other cation (application), is commonly used as a preservative in wines and dried fruits (preserve to S3). It's `S3` with an `O` in the middle, a play on [oxidization](https://wiki.mozilla.org/Oxidation).

## Motivation

The AWS SDK is a little low-level for users to take advantage of the concurrency & parallelism, with the following challenges:

1. You need to orchestrate the parallel multipart download & upload for large files.
2. The built-in retry settings are too low-level, and we allow installing higher-level retries.
3. The async API doesn't agree well with the filesystem, especially for streaming small chunks from/to disk.

To address them, we provide implementations for the parallel multipart download & upload, and higher-level retries. We also make sure the on-disk file is adequately buffered to avoid task-threading overhead.

## License
This project is licensed under the MIT license.
