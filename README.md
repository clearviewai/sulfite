# sulfite

## Overview

`sulfite` is a high-level S3 client built on [AWS SDK for Rust](https://awslabs.github.io/aws-sdk-rust/) for even better ease of use, reliability, and bandwidth saturation (>50 Gbps).

*The name*: `SO3^2-`, an anion, implying a companion to some other cation (application), is commonly used as a preservative in wines and dried fruits (preserve to S3). It's `S3` with an `O` in the middle, a play on [oxidization](https://wiki.mozilla.org/Oxidation).

## Motivation

The AWS SDK is a little low-level for users to take advantage of the concurrency & parallelism, with the following challenges:

1. You need to orchestrate the parallel multipart download & upload for large files.
2. The built-in retry settings are too basic (limited to HTTP status codes, none for bytestream errors), and we allow installing higher-level retries.
3. The async API doesn't agree well with the filesystem for high-throughput operations, when it comes to streaming small chunks from/to disk.

To address them, we provide implementations for the parallel multipart download & upload, and higher-level retries. We also make sure the on-disk file is adequately buffered to avoid async-sync overhead.

## Crates

Workspace for the sulfite S3 ecosystem:

- **[sulfite](sulfite/)** — High-level S3 client library for Rust ([crates.io](https://crates.io/crates/sulfite) \| [docs.rs](https://docs.rs/sulfite))
- **[sulfite-tools](sulfite-tools/)** — CLI for S3 (list, single-object ops, batch ops from CSV) ([crates.io](https://crates.io/crates/sulfite-tools) \| [docs.rs](https://docs.rs/sulfite-tools))

## CLI Installation

```bash
cargo install sulfite-tools
```

## CLI Usage

Global options (can be used with any subcommand):

- `--region`, `-r` — AWS region (or region of custom endpoint)
- `--endpoint-url`, `-e` — S3 endpoint URL (e.g. for MinIO)
- `--max-retries` — Maximum retries per request (default: 3)

### Subcommands

| Command | Description |
|--------|-------------|
| `list` | List objects in a bucket (optional prefix/suffix), output keys to CSV or stdout |
| `head` | Get metadata (HEAD) for one object |
| `download` | Download one object (single request) |
| `download-multipart` | Download one object (multipart transfer) |
| `upload` | Upload one object (single request) |
| `upload-multipart` | Upload one object (multipart transfer) |
| `delete` | Delete one object |
| `copy` | Copy one object from source to destination |
| `restore` | Restore one object from archival storage (e.g. Glacier) |
| `csv` | Run one operation per key from a CSV file (batch) |

### Examples

```bash
# List keys in a bucket with prefix, write to CSV
sulfite list -b my-bucket -p my/prefix/ -o keys.csv

# Get metadata (HEAD) for one object
sulfite head -b my-bucket -k path/to/object

# Download an object
sulfite download -b my-bucket -k path/to/object -l local-file

# Upload with multipart (large files)
sulfite upload-multipart -b my-bucket -k path/to/object -l local-file

# Use custom endpoint (e.g. MinIO)
sulfite -e https://minio.example.com list -b my-bucket -p ""
```

### CSV workflow

Use `list` to write a manifest of keys to a CSV file, then use the `csv` subcommand to run batch operations (e.g. download all objects into a directory):

```bash
# 1. List keys under a prefix and write to a manifest CSV
sulfite list -b my-bucket -p my/prefix/ -o manifest.csv

# 2. Download every key in the manifest to a local directory (keys become paths under that dir)
sulfite csv manifest.csv --has-header download -b my-bucket -p my/prefix/ -l ./downloaded
```

Use `-c/--column-idx N` if the key column is not the first (0-based index).

**CSV skip behavior** — For `csv download` and `csv upload`, an item is skipped if the destination already exists with the same size and a destination timestamp that is not older than the source. Otherwise the existing file or object is overwritten. This avoids re-transferring unchanged files when re-running a batch.

**Multipart activation** — In CSV batch mode, download uses a single GET for objects under 1 GB and multipart for ≥ 1 GB. Upload uses single-part for under 1 GB and multipart for ≥ 1 GB. The single-object commands `download` / `upload` always use one request; `download-multipart` / `upload-multipart` always use multipart.

**Archival and small files** — When you specify an archival storage class (e.g. `--storage-class GLACIER`) on `csv upload` or `csv copy`, objects under 16 KB are stored as STANDARD instead of the requested class, for efficiency.

Run `sulfite --help` or `sulfite <command> --help` for full options.

## License
This project is licensed under the MIT license.
