//! Optional `MinIO` conformance and a large streaming roundtrip.
//! Set `TSD_S3_URL` plus `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` to a local test bucket.

use std::process::{Command, Stdio};
use std::sync::Arc;
use tinystoragedrivers_core::{
    BlobBackend, CompositeStorage, MemoryStorage, PurgeMode, PurgeRequest, Scope, ScopeGeneration,
    StorageBackend,
};
use tinystoragedrivers_s3::{S3Config, S3Storage};
use tokio::io::AsyncReadExt;

fn driver() -> Result<Option<S3Storage>, tinystoragedrivers_core::StorageError> {
    let Ok(url) = std::env::var("TSD_S3_URL") else {
        return Ok(None);
    };
    Ok(Some(S3Storage::open(&S3Config::parse(&url)?)?))
}

#[tokio::test]
async fn live_minio_blob_and_isolation_conformance() {
    let Some(blobs) = driver().unwrap() else {
        return;
    };
    let backend = CompositeStorage::new(Arc::new(MemoryStorage::new()), Arc::new(blobs));
    tinystoragedrivers_core::conformance::run_with_process_capabilities(
        &backend, false, false, false,
    )
    .await;
}

#[tokio::test]
async fn live_minio_composite_purge_deletes_across_pages_and_keeps_sibling_scope() {
    let Some(blobs) = driver().unwrap() else {
        return;
    };
    let blobs = Arc::new(blobs);
    let backend = CompositeStorage::new(Arc::new(MemoryStorage::new()), blobs.clone());
    let target = Scope::new("live-purge-target").unwrap();
    let sibling = Scope::new("live-purge-sibling").unwrap();
    let target_blobs = backend.for_scope(&target).unwrap();
    let sibling_blobs = backend.for_scope(&sibling).unwrap();
    for index in 0..1_005 {
        target_blobs
            .blobs()
            .put(&format!("key-{index:04}"), vec![1], None)
            .await
            .unwrap();
    }
    target_blobs
        .blobs()
        .put("g/1/foo", vec![3], None)
        .await
        .unwrap();
    sibling_blobs
        .blobs()
        .put("keep", vec![2], None)
        .await
        .unwrap();
    let receipt = target_blobs
        .purge(&PurgeRequest {
            operation_id: "minio-purge".into(),
            mode: PurgeMode::Quiescent,
        })
        .await
        .unwrap();
    let replacement = backend
        .for_scope_generation(&target, receipt.generation)
        .unwrap();
    replacement
        .blobs()
        .put("replacement", vec![4], None)
        .await
        .unwrap();
    blobs
        .purge_scope_generation(&target, ScopeGeneration(0))
        .await
        .unwrap();
    assert_eq!(replacement.blobs().list("").await.unwrap().len(), 1);
    assert_eq!(
        replacement
            .blobs()
            .get("replacement")
            .await
            .unwrap()
            .unwrap()
            .bytes,
        [4]
    );
    assert_eq!(
        backend
            .for_scope(&sibling)
            .unwrap()
            .blobs()
            .list("")
            .await
            .unwrap()
            .len(),
        1
    );
}

fn signed_minio_request(
    method: &str,
    url: &str,
    body: Option<&str>,
) -> Result<String, Box<dyn std::error::Error>> {
    let access_key = std::env::var("AWS_ACCESS_KEY_ID")?;
    let secret_key = std::env::var("AWS_SECRET_ACCESS_KEY")?;
    let credentials = format!("{access_key}:{secret_key}");
    let mut command = Command::new("curl");
    command
        .args([
            "-fsS",
            "--aws-sigv4",
            "aws:amz:us-east-1:s3",
            "--user",
            &credentials,
            "-X",
            method,
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    if let Some(body) = body {
        command.args(["-H", "Content-Type: application/xml", "--data-binary", body]);
    }
    command.arg(url);
    let output = command.output()?;
    if !output.status.success() {
        return Err(std::io::Error::other(format!(
            "signed MinIO {method} request failed with status {}",
            output.status
        ))
        .into());
    }
    Ok(String::from_utf8(output.stdout)?)
}

fn create_versioned_bucket(
    url: &str,
) -> Result<(S3Config, String, String), Box<dyn std::error::Error>> {
    let mut config = S3Config::parse(url)?;
    let endpoint = config.endpoint.as_deref().ok_or_else(|| {
        std::io::Error::other("versioned purge requires an explicit local endpoint")
    })?;
    let endpoint_url = url::Url::parse(endpoint)?;
    assert!(
        matches!(
            endpoint_url.host_str(),
            Some("127.0.0.1" | "localhost" | "::1")
        ),
        "versioned-bucket test is restricted to the explicitly configured local MinIO endpoint"
    );
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    config.bucket = format!("tsd-versioned-{}-{nonce}", std::process::id());
    config.prefix = "scope-purge-test".into();
    let bucket_root = format!("{}/{}", endpoint.trim_end_matches('/'), config.bucket);
    signed_minio_request("PUT", &bucket_root, None)?;
    let bucket_url = format!("{bucket_root}?versioning");
    signed_minio_request(
        "PUT",
        &bucket_url,
        Some(
            "<VersioningConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Status>Enabled</Status></VersioningConfiguration>",
        ),
    )?;
    Ok((config, bucket_root, bucket_url))
}

#[tokio::test]
async fn live_minio_scope_purge_removes_all_versions_and_markers_only_for_target()
-> Result<(), Box<dyn std::error::Error>> {
    let Ok(url) = std::env::var("TSD_S3_URL") else {
        return Ok(());
    };
    let (config, bucket_root, bucket_url) = create_versioned_bucket(&url)?;

    let storage = S3Storage::open(&config)?;
    let target = Scope::new("live-versioned-target")?;
    let sibling = Scope::new("live-versioned-sibling")?;
    let target_blobs = storage.for_scope(&target)?;
    let sibling_blobs = storage.for_scope(&sibling)?;
    target_blobs.put("versioned-target", vec![1], None).await?;
    target_blobs.put("versioned-target", vec![2], None).await?;
    assert!(target_blobs.delete("versioned-target").await?);
    sibling_blobs
        .put("versioned-sibling", vec![3], None)
        .await?;

    let target_scope = scope_hex(target.as_str());
    let sibling_scope = scope_hex(sibling.as_str());
    let target_key = format!("{}/scopes/{target_scope}/versioned-target", config.prefix);
    let sibling_key = format!("{}/scopes/{sibling_scope}/versioned-sibling", config.prefix);
    let target_versions_url = format!("{bucket_root}?versions&prefix={target_key}");
    let target_versions_before = signed_minio_request("GET", &target_versions_url, None)?;
    assert_eq!(
        target_versions_before
            .matches(&format!("<Key>{target_key}</Key>"))
            .count(),
        3,
        "the fixture must contain two target versions and their delete marker before purge"
    );
    let sibling_versions_url = format!("{bucket_root}?versions&prefix={sibling_key}");
    let sibling_versions_before = signed_minio_request("GET", &sibling_versions_url, None)?;
    assert!(sibling_versions_before.contains(&format!("<Key>{sibling_key}</Key>")));

    storage.purge_scope(&target).await?;

    let target_versions_after = signed_minio_request("GET", &target_versions_url, None)?;
    assert!(
        !target_versions_after.contains(&format!("<Key>{target_key}</Key>")),
        "purge must physically remove every target version and delete marker"
    );
    let sibling_versions_after = signed_minio_request("GET", &sibling_versions_url, None)?;
    assert!(
        sibling_versions_after.contains(&format!("<Key>{sibling_key}</Key>")),
        "purge must preserve every sibling version"
    );

    signed_minio_request(
        "PUT",
        &bucket_url,
        Some(
            "<VersioningConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Status>Suspended</Status></VersioningConfiguration>",
        ),
    )?;
    target_blobs
        .put("suspended-null-version", vec![4], None)
        .await?;
    let null_key = format!(
        "{}/scopes/{target_scope}/suspended-null-version",
        config.prefix
    );
    let null_versions_url = format!("{bucket_root}?versions&prefix={null_key}");
    let null_versions_before = signed_minio_request("GET", &null_versions_url, None)?;
    assert!(null_versions_before.contains("<VersionId>null</VersionId>"));
    storage.purge_scope(&target).await?;
    assert!(
        !signed_minio_request("GET", &null_versions_url, None)?
            .contains(&format!("<Key>{null_key}</Key>")),
        "purge must remove the null version from a suspended bucket"
    );

    storage.purge_scope(&sibling).await?;
    signed_minio_request("DELETE", &bucket_root, None)?;
    Ok(())
}

fn scope_hex(scope: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(scope.len() * 2);
    for byte in scope.bytes() {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

#[tokio::test]
async fn live_minio_500_mib_streaming_roundtrip() {
    let Some(driver) = driver().unwrap() else {
        return;
    };
    let scope = driver.for_scope(&Scope::new("large").unwrap()).unwrap();
    let len = 500 * 1024 * 1024;
    let meta = scope
        .put_stream(
            "streaming",
            Box::pin(tokio::io::repeat(42).take(len)),
            len,
            Some("application/octet-stream"),
        )
        .await
        .unwrap();
    assert_eq!(meta.len, len);
    let mut blob = scope.get_stream("streaming").await.unwrap().unwrap();
    assert_eq!(blob.meta, meta);
    let mut chunk = vec![0; 64 * 1024];
    let mut got = 0_u64;
    loop {
        let read = blob.reader.read(&mut chunk).await.unwrap();
        if read == 0 {
            break;
        }
        assert!(chunk[..read].iter().all(|b| *b == 42));
        got += read as u64;
    }
    assert_eq!(got, len);
    assert!(scope.delete("streaming").await.unwrap());
}

#[tokio::test]
async fn live_minio_direct_presigned_upload_and_download() {
    let Some(driver) = driver().unwrap() else {
        return;
    };
    let scope = driver.for_scope(&Scope::new("presigned").unwrap()).unwrap();
    let ttl = std::time::Duration::from_secs(60);
    let client = reqwest::Client::new();
    let put = scope.presign_put("direct", ttl).await.unwrap();
    client
        .put(put)
        .body("direct bytes")
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    let get = scope.presign_get("direct", ttl).await.unwrap();
    let body = client
        .get(get)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(body.as_ref(), b"direct bytes");
    assert!(scope.delete("direct").await.unwrap());
}
