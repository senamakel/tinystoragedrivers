//! Compile the additive host-context and routing-descriptor public surface.

use serde::de::DeserializeOwned;
use tinystoragedrivers_busproxy::{HostContext, HostStorageContext, StorageDescriptor};

#[derive(Debug, PartialEq, Eq)]
struct HostProfileContext {
    opaque_profile: u64,
}

impl HostContext for HostProfileContext {}

fn assert_serializable_routing_descriptor<T: serde::Serialize + DeserializeOwned>() {}

#[test]
fn context_is_host_owned_and_descriptor_is_serializable_routing_data() {
    let context = HostStorageContext::new(HostProfileContext { opaque_profile: 17 });
    assert_eq!(context.as_ref().opaque_profile, 17);

    assert_serializable_routing_descriptor::<StorageDescriptor>();
}
