//! Compile and exercise the released storage endpoint API from a downstream test crate.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{Value, json};
use tinybus::BusName;
use tinystoragedrivers_bus::{
    BlobKeyRequest, BlobStreamHead, PutBlobStreamRequest, StorageNeeds, WatchChanged,
};
use tinystoragedrivers_busproxy::{HostFailure, StorageCall, StorageEndpoint};
use tinystoragedrivers_client::{Admission, Object};
use tinystoragedrivers_core::BlobReader;
use tokio::sync::broadcast;

struct LegacyEndpoint;

#[async_trait]
impl StorageEndpoint for LegacyEndpoint {
    async fn call(&self, _call: StorageCall<'_>) -> Result<Value, HostFailure> {
        Ok(json!({"legacy": true}))
    }

    async fn put_blob_stream(
        &self,
        _sender: Option<&BusName>,
        _head: PutBlobStreamRequest,
        _body: BlobReader,
    ) -> Result<Value, HostFailure> {
        Ok(Value::Null)
    }

    async fn get_blob_stream(
        &self,
        _sender: Option<&BusName>,
        _request: BlobKeyRequest,
    ) -> Result<Option<(BlobStreamHead, BlobReader)>, HostFailure> {
        Ok(None)
    }

    fn watch_signals(&self, _sender: &BusName) -> Option<broadcast::Receiver<WatchChanged>> {
        None
    }
}

#[tokio::test]
async fn released_struct_literals_and_object_safe_endpoint_remain_usable() {
    let needs = StorageNeeds {
        collections: &[],
        blob_prefixes: &[],
        stream_prefixes: &[],
        secret_prefixes: &[],
    };
    assert!(needs.collections.is_empty());

    let sender = BusName::new(":1.41").unwrap();
    let call = StorageCall {
        object: Object::Documents,
        selector: "Capabilities",
        request: Value::Null,
        sender: Some(&sender),
        admission: Admission::Private,
    };
    let endpoint: Arc<dyn StorageEndpoint> = Arc::new(LegacyEndpoint);

    assert_eq!(endpoint.call(call).await.unwrap(), json!({"legacy": true}));
}
