//! Version-aware S3 requests used by physical scope purge and enumeration.

use std::{
    collections::HashSet,
    sync::{Arc, OnceLock},
    time::Duration,
};

use http::Method;
use object_store::{
    path::Path,
    signer::{SignedUrlOptions, Signer},
};
use serde::Deserialize;
use tinystoragedrivers_core::{Capability, Result, StorageError};

const REQUEST_TTL: Duration = Duration::from_secs(300);
const PAGE_SIZE: &str = "1000";
const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

fn http_client() -> Result<&'static reqwest::Client> {
    static CLIENT: OnceLock<std::result::Result<reqwest::Client, ()>> = OnceLock::new();
    match CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(REQUEST_TIMEOUT)
            .build()
            .map_err(|_| ())
    }) {
        Ok(client) => Ok(client),
        Err(()) => Err(backend_error("failed to configure bounded S3 HTTP client")),
    }
}

#[derive(Debug, Deserialize)]
struct ListVersionsPage {
    #[serde(rename = "IsTruncated")]
    truncated: bool,
    #[serde(rename = "NextKeyMarker")]
    next_key_marker: Option<String>,
    #[serde(rename = "NextVersionIdMarker")]
    next_version_id_marker: Option<String>,
    #[serde(rename = "Version", default)]
    versions: Vec<VersionedObject>,
    #[serde(rename = "DeleteMarker", default)]
    delete_markers: Vec<VersionedObject>,
}

#[derive(Debug, Deserialize)]
struct VersionedObject {
    #[serde(rename = "Key")]
    key: String,
    #[serde(rename = "VersionId")]
    version_id: String,
}

fn parse_page(body: &str) -> Result<ListVersionsPage> {
    let mut reader = quick_xml::Reader::from_str(body);
    loop {
        match reader.read_event() {
            Ok(quick_xml::events::Event::Start(element))
                if element.local_name().as_ref() == b"ListVersionsResult" =>
            {
                break;
            }
            Ok(
                quick_xml::events::Event::Start(_)
                | quick_xml::events::Event::Empty(_)
                | quick_xml::events::Event::Eof,
            )
            | Err(_) => {
                return Err(backend_error("invalid S3 version-list response"));
            }
            Ok(_) => {}
        }
    }
    let page: ListVersionsPage = quick_xml::de::from_str(body)
        .map_err(|_| backend_error("invalid S3 version-list response"))?;
    if page
        .versions
        .iter()
        .chain(&page.delete_markers)
        .any(|object| object.key.is_empty() || object.version_id.is_empty())
    {
        return Err(backend_error("invalid S3 version-list response"));
    }
    Ok(page)
}

fn next_markers(
    page: &ListVersionsPage,
    key_marker: Option<&str>,
    version_marker: Option<&str>,
) -> Result<(String, Option<String>)> {
    let next_key = page
        .next_key_marker
        .as_deref()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| backend_error("truncated S3 version-list omitted its key marker"))?;
    let next_version = page
        .next_version_id_marker
        .as_deref()
        .filter(|value| !value.is_empty())
        .map(str::to_owned);
    if key_marker == Some(next_key)
        && next_version
            .as_deref()
            .is_none_or(|next| version_marker == Some(next))
    {
        return Err(backend_error("S3 version-list pagination did not advance"));
    }
    Ok((next_key.to_owned(), next_version))
}

fn unsupported_signer() -> StorageError {
    StorageError::unsupported(
        Capability::ScopePurge,
        "version-aware S3 operations require an S3 signer",
    )
}

fn backend_error(message: &'static str) -> StorageError {
    StorageError::backend(message)
}

async fn list_page(
    client: &reqwest::Client,
    signer: &dyn Signer,
    prefix: &str,
    key_marker: Option<&str>,
    version_marker: Option<&str>,
) -> Result<ListVersionsPage> {
    let mut query = vec![
        ("versions".to_owned(), String::new()),
        ("prefix".to_owned(), prefix.to_owned()),
        ("max-keys".to_owned(), PAGE_SIZE.to_owned()),
    ];
    if let Some(marker) = key_marker {
        query.push(("key-marker".to_owned(), marker.to_owned()));
    }
    if let Some(marker) = version_marker {
        query.push(("version-id-marker".to_owned(), marker.to_owned()));
    }
    let url = signer
        .signed_url_opts(
            Method::GET,
            &Path::ROOT,
            REQUEST_TTL,
            &SignedUrlOptions::default().with_query(query),
        )
        .await
        .map_err(|_| backend_error("failed to sign S3 version-list request"))?;
    let response = client
        .get(url)
        .send()
        .await
        .map_err(|_| backend_error("S3 version-list request failed"))?;
    if !response.status().is_success() {
        return Err(backend_error("S3 version-list request was rejected"));
    }
    let body = response
        .text()
        .await
        .map_err(|_| backend_error("failed to read S3 version-list response"))?;
    parse_page(&body)
}

async fn delete_version(
    client: &reqwest::Client,
    signer: &dyn Signer,
    object: VersionedObject,
) -> Result<()> {
    let path = Path::parse(&object.key)
        .map_err(|_| backend_error("invalid key in S3 version-list response"))?;
    let url = signer
        .signed_url_opts(
            Method::DELETE,
            &path,
            REQUEST_TTL,
            &SignedUrlOptions::default().with_query([("versionId", object.version_id)]),
        )
        .await
        .map_err(|_| backend_error("failed to sign S3 version-delete request"))?;
    let response = client
        .delete(url)
        .send()
        .await
        .map_err(|_| backend_error("S3 version-delete request failed"))?;
    if !response.status().is_success() {
        return Err(backend_error("S3 version-delete request was rejected"));
    }
    Ok(())
}

pub(super) async fn purge_prefix(signer: Option<&Arc<dyn Signer>>, prefix: &str) -> Result<()> {
    let signer = signer.ok_or_else(unsupported_signer)?;
    let client = http_client()?;
    let mut key_marker = None;
    let mut version_marker = None;
    let mut seen_markers = HashSet::new();
    loop {
        let page = list_page(
            client,
            signer.as_ref(),
            prefix,
            key_marker.as_deref(),
            version_marker.as_deref(),
        )
        .await?;
        let next = if page.truncated {
            let markers = next_markers(&page, key_marker.as_deref(), version_marker.as_deref())?;
            if !seen_markers.insert(markers.clone()) {
                return Err(backend_error("S3 version-list pagination marker cycled"));
            }
            Some(markers)
        } else {
            None
        };
        let mut objects = page.versions;
        objects.extend(page.delete_markers);
        for object in objects {
            if !object.key.starts_with(prefix) {
                return Err(backend_error(
                    "S3 version-list returned an out-of-prefix key",
                ));
            }
            delete_version(client, signer.as_ref(), object).await?;
        }
        let Some((next_key, next_version)) = next else {
            return Ok(());
        };
        key_marker = Some(next_key);
        version_marker = next_version;
    }
}

pub(super) async fn list_keys(
    signer: Option<&Arc<dyn Signer>>,
    prefix: &str,
) -> Result<Vec<String>> {
    let signer = signer.ok_or_else(unsupported_signer)?;
    let client = http_client()?;
    let mut key_marker = None;
    let mut version_marker = None;
    let mut keys = Vec::new();
    let mut seen_markers = HashSet::new();
    loop {
        let page = list_page(
            client,
            signer.as_ref(),
            prefix,
            key_marker.as_deref(),
            version_marker.as_deref(),
        )
        .await?;
        let next = if page.truncated {
            let markers = next_markers(&page, key_marker.as_deref(), version_marker.as_deref())?;
            if !seen_markers.insert(markers.clone()) {
                return Err(backend_error("S3 version-list pagination marker cycled"));
            }
            Some(markers)
        } else {
            None
        };
        for object in page.versions.iter().chain(&page.delete_markers) {
            if !object.key.starts_with(prefix) {
                return Err(backend_error(
                    "S3 version-list returned an out-of-prefix key",
                ));
            }
            keys.push(object.key.clone());
        }
        let Some((next_key, next_version)) = next else {
            return Ok(keys);
        };
        key_marker = Some(next_key);
        version_marker = next_version;
    }
}

#[cfg(test)]
#[path = "versions_tests.rs"]
mod tests;
