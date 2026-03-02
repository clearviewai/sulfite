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
2. The built-in retry settings are too basic (limited to HTTP status codes, none for bytestream errors), and we allow installing higher-level retries.
3. The async API doesn't agree well with the filesystem for high-throughput operations, when it comes to streaming small chunks from/to disk.

To address them, we provide implementations for the parallel multipart download & upload, and higher-level retries. We also make sure the on-disk file is adequately buffered to avoid async-sync overhead.

**Low-level access** — `S3Client` exposes the underlying AWS SDK client as the public `inner` field so you can call SDK operations not covered by this crate.

## Testing

Integration tests use [LocalStack](https://docs.localstack.cloud/) for S3.

- **Local:** Start LocalStack, then run the ignored integration tests:
  ```bash
  docker run --rm -it -p 4566:4566 -e SERVICES=s3 localstack/localstack
  cargo test -p sulfite --test localstack -- --ignored
  ```
- **CI:** GitHub Actions runs LocalStack as a service container and runs the same tests (see `.github/workflows/ci.yml`).
