//! `SecretStore` over the confidential `Secrets` object.
use tinystoragedrivers_bus as wire;
use tinystoragedrivers_core::{Capability, Result, async_trait};
use tinystoragedrivers_secrets::{Precondition, SecretStore, SecretVersion, Zeroizing};

use super::Wire;
use crate::transport::Object;

/// The secret port over a bus. Its debug output carries nothing.
#[derive(Clone)]
pub struct BusSecrets {
    wire: Wire,
}

impl std::fmt::Debug for BusSecrets {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BusSecrets").finish_non_exhaustive()
    }
}

impl BusSecrets {
    pub(super) fn new(wire: Wire) -> Self {
        Self { wire }
    }
}

#[async_trait]
impl SecretStore for BusSecrets {
    async fn get(&self, name: &str) -> Result<Option<Zeroizing<Vec<u8>>>> {
        let request = wire::SecretNameRequest {
            name: name.to_owned(),
        };
        let found: Option<wire::SecretValue> = self
            .wire
            .call(Object::Secrets, "Get", &request, None)
            .await?;
        Ok(found.map(|secret| Zeroizing::new(secret.value)))
    }

    async fn set(&self, name: &str, value: &[u8]) -> Result<()> {
        let request = wire::SetSecretRequest {
            name: name.to_owned(),
            value: value.to_vec(),
        };
        self.wire.call(Object::Secrets, "Set", &request, None).await
    }

    async fn get_versioned(
        &self,
        name: &str,
    ) -> Result<Option<(Zeroizing<Vec<u8>>, SecretVersion)>> {
        let request = wire::SecretNameRequest {
            name: name.to_owned(),
        };
        let found: Option<wire::VersionedSecret> = self
            .wire
            .call(
                Object::Secrets,
                "GetVersioned",
                &request,
                Some(Capability::ConditionalSecretWrites),
            )
            .await?;
        Ok(found.map(|secret| {
            (
                Zeroizing::new(secret.value),
                SecretVersion::from_bytes(secret.version),
            )
        }))
    }

    async fn set_if(
        &self,
        name: &str,
        value: &[u8],
        condition: Precondition,
    ) -> Result<SecretVersion> {
        let condition = match condition {
            Precondition::Absent => wire::SecretPrecondition::Absent,
            Precondition::Version(version) => {
                wire::SecretPrecondition::Version(wire::SecretRevision(version.into_bytes()))
            }
        };
        let request = wire::SetSecretIfRequest {
            name: name.to_owned(),
            value: value.to_vec(),
            condition,
        };
        let revision: wire::SecretRevision = self
            .wire
            .call(
                Object::Secrets,
                "SetIf",
                &request,
                Some(Capability::ConditionalSecretWrites),
            )
            .await?;
        Ok(SecretVersion::from_bytes(revision.0))
    }

    async fn delete(&self, name: &str) -> Result<bool> {
        let request = wire::SecretNameRequest {
            name: name.to_owned(),
        };
        self.wire
            .call(Object::Secrets, "Delete", &request, None)
            .await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let request = wire::ListSecretsRequest {
            prefix: prefix.to_owned(),
        };
        self.wire
            .call(Object::Secrets, "List", &request, None)
            .await
    }

    fn backend_name(&self) -> &'static str {
        "bus"
    }
}
