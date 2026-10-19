//! Independently configured blob storage and composite document/blob backends.
#[cfg(not(feature = "s3"))]
use crate::StorageError;
use crate::{BackendBlobs, BlobBackend, CompositeStorage, Result, StorageBackend, StorageConfig};
use std::fmt;
use std::sync::Arc;

/// A blob location independent of the document/stream location.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlobConfig {
    /// The blob port from a complete backend.
    Backend(StorageConfig),
    /// An S3-compatible object-store URL. Credentials belong in the provider chain.
    S3 {
        /// Validated S3 location without credentials.
        url: String,
    },
}

impl BlobConfig {
    /// Parse a blob URL (s3://, or any [`StorageConfig`] form).
    ///
    /// # Errors
    ///
    /// Invalid URL, or an S3 URL when the s3 feature is absent.
    pub fn parse(url: &str) -> Result<Self> {
        if url.starts_with("s3://") {
            #[cfg(feature = "s3")]
            {
                tinystoragedrivers_s3::S3Config::parse(url)?;
                return Ok(Self::S3 {
                    url: url.to_owned(),
                });
            }
            #[cfg(not(feature = "s3"))]
            return Err(StorageError::invalid_input(
                "s3 blob URL needs the s3 feature",
            ));
        }
        let config = StorageConfig::parse(url)?;
        if matches!(&config, StorageConfig::Composite { .. }) {
            return Err(crate::StorageError::invalid_input(
                "nested composite blob URLs are not supported",
            ));
        }
        Ok(Self::Backend(config))
    }
}

impl fmt::Display for BlobConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Backend(config) => write!(f, "{config}"),
            Self::S3 { url } => f.write_str(url),
        }
    }
}

/// Open a scoped blob backend independent of the document backend.
///
/// # Errors
///
/// A missing feature, invalid configuration, or backend connection error.
pub async fn open_blobs(config: &BlobConfig) -> Result<Arc<dyn BlobBackend>> {
    match config {
        BlobConfig::Backend(config) => Ok(Arc::new(BackendBlobs::new(
            Box::pin(crate::open(config)).await?,
        ))),
        #[cfg(feature = "s3")]
        BlobConfig::S3 { url } => Ok(Arc::new(tinystoragedrivers_s3::S3Storage::open(
            &tinystoragedrivers_s3::S3Config::parse(url)?,
        )?)),
        #[cfg(not(feature = "s3"))]
        BlobConfig::S3 { .. } => Err(StorageError::invalid_input(
            "s3 blob URL needs the s3 feature",
        )),
    }
}

/// Open documents/streams and blobs on independent locations.
///
/// Use an unfenced scoped blob handle to upload first, then a fenced document
/// handle to publish the reference. Fenced composite blob writes are refused.
///
/// # Errors
///
/// Any parse/connection failure in either configured backend.
pub async fn open_composite(
    records: &StorageConfig,
    blobs: &BlobConfig,
) -> Result<Arc<dyn StorageBackend>> {
    Ok(Arc::new(CompositeStorage::new(
        Box::pin(crate::open(records)).await?,
        open_blobs(blobs).await?,
    )))
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
