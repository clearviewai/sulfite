//! LocalStack integration tests for sulfite.
//!
//! Requires LocalStack running (e.g. `docker run --rm -it -p 4566:4566 localstack/localstack`).
//! Set `LOCALSTACK_ENDPOINT` to override default `http://localhost:4566`.
//!
//! Each test uses random hex in bucket or prefix so tests are independent and can run in parallel.
//! Cleanup is best-effort; the LocalStack container can be removed to reset state.

use rstest::rstest;
use sulfite::{generate_random_hex, RetryConfig, S3Client, S3ClientConfig, S3Error};

const DEFAULT_LOCALSTACK_ENDPOINT: &str = "http://localhost:4566";
const TEST_BUCKET: &str = "sulfite-test-bucket";
const RANDOM_HEX_LEN: usize = 32;
/// Parent dir under temp_dir() for all LocalStack test local files (uploads/downloads). Each test uses a subdir keyed by run.
const LOCALSTACK_TEST_TEMP_DIR: &str = "sulfite_localstack_test";

fn localstack_endpoint() -> String {
    std::env::var("LOCALSTACK_ENDPOINT").unwrap_or_else(|_| DEFAULT_LOCALSTACK_ENDPOINT.into())
}

async fn make_client() -> S3Client {
    let endpoint = localstack_endpoint();
    let config = S3ClientConfig {
        endpoint_url: Some(endpoint),
        region: Some("us-east-1".into()),
        access_secret_session_tuple: Some(("test".into(), "test".into(), None)),
        ..S3ClientConfig::default()
    };
    S3Client::new(config, RetryConfig::default()).await
}

async fn ensure_bucket(client: &S3Client, bucket: &str) {
    let _ = client.create_bucket(bucket).await;
}

/// Base path for a test's local upload/download files: temp_dir() / LOCALSTACK_TEST_TEMP_DIR / run. Use join("upload")|join("download") or join(filename) as needed.
fn localstack_test_base(run: &str) -> std::path::PathBuf {
    std::env::temp_dir()
        .join(LOCALSTACK_TEST_TEMP_DIR)
        .join(run)
}

// Create and delete bucket
#[tokio::test]
#[ignore = "requires LocalStack (run with: cargo test --test localstack -- --ignored)"]
async fn create_and_delete_bucket() {
    let client = make_client().await;
    let bucket = format!(
        "sulfite-create-delete-{}",
        generate_random_hex(RANDOM_HEX_LEN)
    );
    client.create_bucket(&bucket).await.expect("create_bucket");
    let bucket_info = client.head_bucket(&bucket).await.expect("head_bucket");
    println!("bucket_info: {:?}", bucket_info);
    assert_eq!(bucket_info.name, bucket);
    assert_eq!(bucket_info.region, Some("us-east-1".into()));
    client.delete_bucket(&bucket).await.expect("delete_bucket");
    let err = client.head_bucket(&bucket).await.unwrap_err();
    if let S3Error::AWSS3Error(_, _, _, code) = err {
        assert_eq!(code, 404, "head after delete must 404");
    } else {
        panic!("expected AWSS3Error 404, got {:?}", err);
    }
}

// Put, head, get (with ObjectInfo checks), copy, head copy, delete original, head 404, delete copy
#[tokio::test]
#[ignore = "requires LocalStack (run with: cargo test --test localstack -- --ignored)"]
async fn put_head_get_copy_delete_flow() {
    let client = make_client().await;
    ensure_bucket(&client, TEST_BUCKET).await;
    let run = generate_random_hex(RANDOM_HEX_LEN);
    let key = format!("put-head-get-copy/{}/test-key", run);
    let key_copied = format!("put-head-get-copy/{}/test-key-copy", run);
    let body = b"hello localstack";

    // Put then head and assert ObjectInfo
    client
        .put_object(TEST_BUCKET, &key, body, None)
        .await
        .expect("put_object");

    // Head and assert ObjectInfo
    let head_info = client
        .head_object(TEST_BUCKET, &key)
        .await
        .expect("head_object after put");
    assert_eq!(head_info.key, key);
    assert_eq!(head_info.size, body.len() as u64);

    // Get and assert body and ObjectInfo
    let (get_info, get_body) = client
        .get_object(TEST_BUCKET, &key, None)
        .await
        .expect("get_object");
    assert_eq!(get_info.key, key);
    assert_eq!(get_info.size, body.len() as u64);
    assert_eq!(get_body.as_slice(), body);

    // Copy to another key and head the copy
    client
        .copy_object(TEST_BUCKET, &key, TEST_BUCKET, &key_copied, None)
        .await
        .expect("copy_object");

    let copy_head_info = client
        .head_object(TEST_BUCKET, &key_copied)
        .await
        .expect("head_object on copied key");
    assert_eq!(copy_head_info.key, key_copied);
    assert_eq!(copy_head_info.size, body.len() as u64);

    // Delete original then head original must 404
    let _ = client.delete_object(TEST_BUCKET, &key).await;
    let err = client.head_object(TEST_BUCKET, &key).await.unwrap_err();
    if let S3Error::AWSS3Error(_, _, _, code) = err {
        assert_eq!(code, 404, "head after delete must 404");
    } else {
        panic!("expected AWSS3Error 404, got {:?}", err);
    }

    let _ = client.delete_object(TEST_BUCKET, &key_copied).await;
}

// Put object then get with byte range (start, end) exclusive; assert exact slice
#[tokio::test]
#[ignore = "requires LocalStack (run with: cargo test --test localstack -- --ignored)"]
async fn get_object_byte_range() {
    let client = make_client().await;
    ensure_bucket(&client, TEST_BUCKET).await;
    let run = generate_random_hex(RANDOM_HEX_LEN);
    let key = format!("get-byte-range/{}/obj", run);
    let body: Vec<u8> = (0..100).map(|i| i as u8).collect(); // 100 bytes 0..99

    client
        .put_object(TEST_BUCKET, &key, &body, None)
        .await
        .expect("put_object");

    // get_object(..., Some((10, 20))) returns bytes 10..20 (end exclusive) = 10 bytes
    let (info, range_body) = client
        .get_object(TEST_BUCKET, &key, Some((10, 20)))
        .await
        .expect("get_object range");
    assert_eq!(info.key, key);
    assert_eq!(info.size, 10);
    assert_eq!(range_body.len(), 10);
    assert_eq!(range_body.as_slice(), &body[10..20]);

    // Invalid range (end <= start) returns ValidationError
    let err = client
        .get_object(TEST_BUCKET, &key, Some((20, 10)))
        .await
        .unwrap_err();
    if let S3Error::ValidationError(msg) = &err {
        assert!(msg.contains("start_end_offsets") || msg.contains("non-positive"));
    } else {
        panic!("expected ValidationError for invalid range, got {:?}", err);
    }
    let err2 = client
        .get_object(TEST_BUCKET, &key, Some((10, 10)))
        .await
        .unwrap_err();
    if let S3Error::ValidationError(_) = err2 {
    } else {
        panic!(
            "expected ValidationError for zero-length range, got {:?}",
            err2
        );
    }

    let _ = client.delete_object(TEST_BUCKET, &key).await;
}

// Put many keys under the same prefix, then get all
#[tokio::test]
#[ignore = "requires LocalStack (run with: cargo test --test localstack -- --ignored)"]
async fn put_1500_keys_then_get_all() {
    let client = make_client().await;
    ensure_bucket(&client, TEST_BUCKET).await;
    let run = generate_random_hex(RANDOM_HEX_LEN);
    let prefix = format!("put-1500-keys/{}/keys/", run);
    let n = 1500;

    for i in 0..n {
        let key = format!("{}k{:05}", prefix, i);
        let body = format!("body-{}", i);
        client
            .put_object(TEST_BUCKET, &key, body.as_bytes(), None)
            .await
            .expect("put_object");
    }

    for i in 0..n {
        let key = format!("{}k{:05}", prefix, i);
        let expected = format!("body-{}", i);
        let (info, body) = client
            .get_object(TEST_BUCKET, &key, None)
            .await
            .expect("get_object");
        assert_eq!(info.key, key);
        assert_eq!(info.size, expected.len() as u64);
        assert_eq!(body, expected.as_bytes(), "key {}", key);
    }

    for i in 0..n {
        let _ = client
            .delete_object(TEST_BUCKET, &format!("{}k{:05}", prefix, i))
            .await;
    }
}

// List prefix and count objects and common prefixes; compare paginated vs iterator (self-contained)
#[tokio::test]
#[ignore = "requires LocalStack (run with: cargo test --test localstack -- --ignored)"]
async fn list_prefix_count_objects_and_prefixes() {
    let client = make_client().await;
    ensure_bucket(&client, TEST_BUCKET).await;
    let run = generate_random_hex(RANDOM_HEX_LEN);
    let prefix_keys = format!("list-prefix-count/{}/keys/", run);
    let prefix_dirs = format!("list-prefix-count/{}/dirs/", run);
    let n_keys = 1500;
    let n_dirs = 1500;

    // Create keys and directories
    for i in 0..n_keys {
        let key = format!("{}k{:05}", prefix_keys, i);
        let body = format!("body-{}", i);
        client
            .put_object(TEST_BUCKET, &key, body.as_bytes(), None)
            .await
            .expect("put");
    }
    for i in 0..n_dirs {
        let key = format!("{}d{:05}/obj", prefix_dirs, i);
        let body = format!("dir-{}", i);
        client
            .put_object(TEST_BUCKET, &key, body.as_bytes(), None)
            .await
            .expect("put");
    }

    // With delimiter "/", listing prefix_keys returns n_keys objects and 0 common_prefixes
    let (objs_pag, prefixes_pag) = client
        .list_objects_v2_paginated(TEST_BUCKET, &prefix_keys, Some("/"))
        .await
        .expect("list_objects_v2_paginated keys");
    assert_eq!(objs_pag.len(), n_keys, "paginated keys: object count");
    assert_eq!(prefixes_pag.len(), 0, "paginated keys: prefix count");
    if let Some(first) = objs_pag.first() {
        assert_eq!(first.key, format!("{}k00000", prefix_keys));
        assert_eq!(first.size, 6); // "body-0".len()
    }

    let mut iter_objs = 0_usize;
    let mut iter_prefixes = 0_usize;
    let mut iter = client.list_objects_v2_page_iter(TEST_BUCKET, &prefix_keys, Some("/"));
    while let Some(page) = iter.next_page().await.expect("next_page keys") {
        iter_objs += page.0.len();
        iter_prefixes += page.1.len();
    }
    assert_eq!(iter_objs, n_keys, "iterator keys: object count");
    assert_eq!(iter_prefixes, 0, "iterator keys: prefix count");

    // With delimiter "/", listing prefix_dirs returns 0 objects (keys are under d00000/, etc.) and n_dirs common_prefixes
    let (objs_pag2, prefixes_pag2) = client
        .list_objects_v2_paginated(TEST_BUCKET, &prefix_dirs, Some("/"))
        .await
        .expect("list_objects_v2_paginated dirs");
    assert_eq!(
        objs_pag2.len(),
        0,
        "delimiter lists roll up into common_prefixes"
    );
    assert_eq!(
        prefixes_pag2.len(),
        n_dirs,
        "delimiter lists roll up into common_prefixes"
    );

    let mut iter_objs2 = 0_usize;
    let mut iter_prefixes2 = 0_usize;
    let mut iter2 = client.list_objects_v2_page_iter(TEST_BUCKET, &prefix_dirs, Some("/"));
    while let Some(page) = iter2.next_page().await.expect("next_page dirs") {
        iter_objs2 += page.0.len();
        iter_prefixes2 += page.1.len();
    }
    assert_eq!(
        iter_objs2, 0,
        "delimiter lists roll up into common_prefixes"
    );
    assert_eq!(
        iter_prefixes2, n_dirs,
        "delimiter lists roll up into common_prefixes"
    );

    // Clean up
    for i in 0..n_keys {
        let _ = client
            .delete_object(TEST_BUCKET, &format!("{}k{:05}", prefix_keys, i))
            .await;
    }
    for i in 0..n_dirs {
        let _ = client
            .delete_object(TEST_BUCKET, &format!("{}d{:05}/obj", prefix_dirs, i))
            .await;
    }
}

// List prefix without delimiter; all keys under prefix returned (flat list)
#[tokio::test]
#[ignore = "requires LocalStack (run with: cargo test --test localstack -- --ignored)"]
async fn list_prefix_no_delimiter() {
    let client = make_client().await;
    ensure_bucket(&client, TEST_BUCKET).await;
    let run = generate_random_hex(RANDOM_HEX_LEN);
    let prefix = format!("list-no-delimiter/{}/", run);
    let n = 50;

    for i in 0..n {
        let key = format!("{}k{:03}", prefix, i);
        let body = format!("body-{}", i);
        client
            .put_object(TEST_BUCKET, &key, body.as_bytes(), None)
            .await
            .expect("put_object");
    }

    let (objects, common_prefixes) = client
        .list_objects_v2_paginated(TEST_BUCKET, &prefix, None)
        .await
        .expect("list_objects_v2_paginated no delimiter");
    assert_eq!(objects.len(), n, "flat list returns all keys");
    assert_eq!(
        common_prefixes.len(),
        0,
        "no delimiter so no common_prefixes"
    );
    if let Some(first) = objects.first() {
        assert_eq!(first.key, format!("{}k000", prefix));
        assert_eq!(first.size, 6); // "body-0".len()
    }

    let mut iter_count = 0_usize;
    let mut iter = client.list_objects_v2_page_iter(TEST_BUCKET, &prefix, None);
    while let Some(page) = iter.next_page().await.expect("next_page") {
        iter_count += page.0.len();
    }
    assert_eq!(iter_count, n, "iterator no delimiter returns all keys");

    for i in 0..n {
        let _ = client
            .delete_object(TEST_BUCKET, &format!("{}k{:03}", prefix, i))
            .await;
    }
}

// Upload objects under a prefix, download each and verify content
#[tokio::test]
#[ignore = "requires LocalStack (run with: cargo test --test localstack -- --ignored)"]
async fn upload_10_download_and_verify() {
    let client = make_client().await;
    ensure_bucket(&client, TEST_BUCKET).await;
    let run = generate_random_hex(RANDOM_HEX_LEN);
    let prefix = format!("upload-download-verify/{}/", run);
    let n = 10;

    let base = localstack_test_base(&run);
    let upload_dir = base.join("upload");
    let download_dir = base.join("download");
    let _ = tokio::fs::create_dir_all(&upload_dir).await;
    let _ = tokio::fs::create_dir_all(&download_dir).await;

    let keys: Vec<String> = (0..n).map(|i| format!("{}obj{:03}", prefix, i)).collect();
    let bodies: Vec<Vec<u8>> = (0..n)
        .map(|i| format!("random-content-{}", i).into_bytes())
        .collect();

    for (key, body) in keys.iter().zip(bodies.iter()) {
        let upload_path = upload_dir.join(key.replace('/', "_"));
        tokio::fs::write(&upload_path, body)
            .await
            .expect("write upload file");
        client
            .upload_object(TEST_BUCKET, key, upload_path.to_str().unwrap(), None)
            .await
            .expect("upload_object");
    }

    for (key, expected) in keys.iter().zip(bodies.iter()) {
        let download_path = download_dir.join(key.replace('/', "_"));
        let info = client
            .download_object(TEST_BUCKET, key, download_path.to_str().unwrap(), None)
            .await
            .expect("download_object");
        assert_eq!(info.key, *key);
        assert_eq!(info.size, expected.len() as u64);
        let got = tokio::fs::read(&download_path).await.expect("read file");
        assert_eq!(got.as_slice(), expected.as_slice(), "key {}", key);
    }

    for key in &keys {
        let _ = client.delete_object(TEST_BUCKET, key).await;
    }
    let _ = tokio::fs::remove_dir_all(&base).await;
}

// Put in archival tier (GLACIER), get fails, restore, then get succeeds (best-effort on LocalStack)
#[tokio::test]
#[ignore = "requires LocalStack (run with: cargo test --test localstack -- --ignored)"]
async fn put_glacier_get_fails_restore_then_get_succeeds() {
    let client = make_client().await;
    ensure_bucket(&client, TEST_BUCKET).await;
    let run = generate_random_hex(RANDOM_HEX_LEN);
    let key = format!("glacier-restore/{}/archived", run);
    let body = b"cold storage content";

    // Put in archival tier (GLACIER)
    client
        .put_object(TEST_BUCKET, &key, body, Some("GLACIER"))
        .await
        .expect("put_object GLACIER");

    // Get and assert failure
    let get_err = client
        .get_object(TEST_BUCKET, &key, None)
        .await
        .unwrap_err();
    if let S3Error::AWSS3Error(_, _, error_meta, code) = get_err {
        assert_eq!(error_meta.code(), Some("InvalidObjectState"));
        assert_eq!(
            code, 403,
            "get before restore should fail with 403 InvalidObjectState"
        );
    }

    // Head and assert ObjectInfo after put
    let head_info = client
        .head_object(TEST_BUCKET, &key)
        .await
        .expect("head_object after put");
    println!("head_info: {:?}", head_info);

    // Restore object
    client
        .restore_object(TEST_BUCKET, &key, 1, "Expedited")
        .await
        .expect("restore_object");

    // Head and assert ObjectInfo after restore
    let head_info = client
        .head_object(TEST_BUCKET, &key)
        .await
        .expect("head_object after restore");
    println!("head_info: {:?}", head_info);

    // Get and assert body and ObjectInfo after restore
    let (info, got) = client
        .get_object(TEST_BUCKET, &key, None)
        .await
        .expect("get_object after restore");
    assert_eq!(info.key, key);
    assert_eq!(info.size, body.len() as u64);
    assert_eq!(got.as_slice(), body);

    let _ = client.delete_object(TEST_BUCKET, &key).await;
}

// Multipart upload then multipart download per size; each case runs as a separate test (parallel). Run with: cargo test -p sulfite --test localstack -- --ignored
#[rstest]
#[case(1_u64)]
#[case(10_u64)]
#[case(42_u64)]
#[case(100_u64)]
#[tokio::test]
#[ignore = "requires LocalStack (run with: cargo test --test localstack -- --ignored)"]
async fn multipart_upload_download_size(#[case] size_mb: u64) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    const CHUNK: usize = 1024 * 1024; // 1 MiB for chunked compare

    let size = size_mb * 1024 * 1024;
    let client = make_client().await;
    ensure_bucket(&client, TEST_BUCKET).await;
    let run = generate_random_hex(RANDOM_HEX_LEN);
    let key = format!("multipart/{}/{}mb", run, size_mb);

    let base = localstack_test_base(&run);
    let _ = tokio::fs::create_dir_all(&base).await;
    let upload_path = base.join(format!("upload_{}mb.dat", size_mb));
    let download_path = base.join(format!("download_{}mb.dat", size_mb));

    // Build source file: 8 KiB repeating pattern 0,1,...,255,0,1,... so content is deterministic and we avoid allocating full size
    let mut file = tokio::fs::File::create(&upload_path)
        .await
        .expect("create upload file");
    let pattern: Vec<u8> = (0..8192).map(|i| (i % 256) as u8).collect();
    let mut written: u64 = 0;
    while written < size {
        let n = (size - written).min(pattern.len() as u64) as usize;
        file.write_all(&pattern[..n]).await.expect("write upload");
        written += n as u64;
    }
    file.flush().await.expect("flush upload file");
    drop(file);

    // Multipart upload to S3
    client
        .upload_object_multipart(
            TEST_BUCKET,
            &key,
            upload_path.to_str().unwrap(),
            None,
            None::<&sulfite::NoopProgressBar>,
        )
        .await
        .expect("upload_object_multipart");

    // Head object and assert size
    let up_info = client
        .head_object(TEST_BUCKET, &key)
        .await
        .expect("head_object after upload");
    assert_eq!(up_info.size, size, "size {} MiB", size_mb);

    // Multipart download to second file
    client
        .download_object_multipart(
            TEST_BUCKET,
            &key,
            download_path.to_str().unwrap(),
            None::<&sulfite::NoopProgressBar>,
        )
        .await
        .expect("download_object_multipart");

    // Assert downloaded file size then byte-wise equality with source (chunked to avoid loading into memory)
    let down_meta = tokio::fs::metadata(&download_path)
        .await
        .expect("metadata download");
    assert_eq!(down_meta.len(), size, "download size {} MiB", size_mb);

    let mut up_file = tokio::fs::File::open(&upload_path)
        .await
        .expect("open upload");
    let mut down_file = tokio::fs::File::open(&download_path)
        .await
        .expect("open download");
    // Heap-allocate chunk buffers to avoid stack overflow (CHUNK is 1 MiB)
    let mut up_buf = vec![0u8; CHUNK];
    let mut down_buf = vec![0u8; CHUNK];
    loop {
        let n_up = up_file.read(&mut up_buf).await.expect("read upload chunk");
        let n_down = down_file
            .read(&mut down_buf)
            .await
            .expect("read download chunk");
        assert_eq!(n_up, n_down, "chunk length mismatch {} MiB", size_mb);
        if n_up == 0 {
            break;
        }
        assert_eq!(
            &up_buf[..n_up],
            &down_buf[..n_down],
            "chunk content mismatch {} MiB",
            size_mb
        );
    }

    // Cleanup: object and temp dir (upload/download files)
    let _ = client.delete_object(TEST_BUCKET, &key).await;
    let _ = tokio::fs::remove_dir_all(&base).await;
}
