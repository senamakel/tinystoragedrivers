//! Confidential schema discovery and owner-transform exchange for `TinyBus` modules.
//!
//! The module adapter reuses TSD's versioned schema/exchange DTOs and generic
//! delivery engine. It authorizes each call with the loader-issued selected
//! host before deriving the peer identity used by TSD's transfer state.
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use tinybus::{
    BusName, CallContext, Connection, Interface, InterfaceName, MemberName, ObjectPath, Proxy,
};
use tinybus_module::{ModuleSetupContext, TrustedHost};
use tinystoragedrivers_core::{
    StorageError,
    schema::{
        Registry, Schema,
        delivery::{
            DEFAULT_MAX_IN_FLIGHT, ExchangeClock, MIGRATION_EXCHANGE_VERSION,
            MigrationExchangeReply, MigrationExchangeRequest, MigrationExchangeServer,
            MigrationTransport, PeerIdentity, TransferIds,
        },
    },
};

pub use tinystoragedrivers_core::schema::delivery::{
    MigrationPayload, MigrationPayloadHandler, MigrationPayloadReply,
};
pub use tinystoragedrivers_core::schema::{
    BlobTransformOutput, BlobTransformRequest, DocumentsTransformRequest, Expiry, Item, Owner,
    SchemaDeclaration, ScopeClass, SourceRecord, StepDeclaration, TargetRecord,
    TransformDescriptor, TransformError, TransformErrorKind, TransformKind, TransformResult,
};
pub use tinystoragedrivers_core::{CollectionSpec, IndexSpec, SearchSpec};

/// `TinyBus` object path for the TSD-owned module migration service.
pub const MIGRATION_OBJECT_PATH: &str = "/org/tinystoragedrivers/migration";
/// `TinyBus` interface for the TSD-owned module migration service.
pub const MIGRATION_INTERFACE: &str = "org.tinystoragedrivers.Migration";
/// Member that returns the module owner's schema and exchange version.
pub const GET_SCHEMA_MEMBER: &str = "GetSchema";
/// Member that accepts one versioned TSD exchange request.
pub const EXCHANGE_MEMBER: &str = "Exchange";

const CLEANUP_INTERVAL: Duration = Duration::from_secs(1);

/// Register the module-owned schema and transform service during plain setup.
///
/// The module must call this before any storage-dependent setup. Registration
/// does not acquire storage, prepare a backend, or publish a storage lease.
/// Setup fails when the loader did not supply a trusted selected-host binding.
/// The SDK-owned connection remains owned by the module runtime.
///
/// # Errors
/// Returns a static `TinyBus` failure when the trusted binding is absent, the
/// declaration is invalid, the runtime cannot start the cleanup reaper, or
/// the migration interface cannot be registered.
pub async fn serve_module_migration(
    setup: &ModuleSetupContext,
    declaration: SchemaDeclaration,
    handler: Arc<dyn MigrationPayloadHandler>,
) -> tinybus::Result<()> {
    let service = new_migration_service(setup.trusted_host().cloned(), declaration, handler)?;
    setup
        .connection()
        .serve_at(ObjectPath::new(MIGRATION_OBJECT_PATH)?, service)
        .await
}

fn new_migration_service(
    trusted_host: Option<TrustedHost>,
    declaration: SchemaDeclaration,
    handler: Arc<dyn MigrationPayloadHandler>,
) -> tinybus::Result<MigrationService> {
    let trusted_host = trusted_host
        .ok_or_else(|| tinybus::Error::failed("trusted migration host binding unavailable"))?;
    validate_declaration(&declaration)?;
    let server = MigrationExchangeServer::new(
        handler,
        Arc::new(MonotonicClock::new()),
        Arc::new(UlidTransferIds),
        DEFAULT_MAX_IN_FLIGHT,
    );
    let cleanup = server
        .cleanup_guard(CLEANUP_INTERVAL)
        .map_err(|_| tinybus::Error::failed("migration cleanup unavailable"))?;
    Ok(MigrationService {
        trusted_host,
        declaration,
        server,
        _cleanup: cleanup,
        interface: InterfaceName::new(MIGRATION_INTERFACE)?,
        members: vec![
            MemberName::new(GET_SCHEMA_MEMBER)?,
            MemberName::new(EXCHANGE_MEMBER)?,
        ],
    })
}

/// Confidential host client for one compiled module's TSD migration service.
///
/// Create it with the exact selected-controller `Connection`; it opens no
/// connection and does not trust a peer identity supplied by the caller.
pub struct MigrationModuleClient {
    proxy: Proxy,
    owner: String,
    declaration: SchemaDeclaration,
}

impl std::fmt::Debug for MigrationModuleClient {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MigrationModuleClient")
            .field("owner", &self.owner)
            .finish_non_exhaustive()
    }
}

impl MigrationModuleClient {
    /// Discover and validate a module's schema using the supplied connection.
    ///
    /// # Errors
    /// Returns a sanitized `Unavailable` error for transport, confidentiality,
    /// protocol-version, or declaration-validation failures.
    pub async fn discover(
        controller: &Connection,
        module_peer: &BusName,
    ) -> tinystoragedrivers_core::Result<Self> {
        let proxy = controller
            .proxy(
                module_peer.as_str(),
                MIGRATION_OBJECT_PATH,
                MIGRATION_INTERFACE,
            )
            .map_err(|_| unavailable())?;
        let announcement: SchemaAnnouncement = proxy
            .call_confidential(GET_SCHEMA_MEMBER, ())
            .await
            .map_err(|_| unavailable())?;
        if announcement.exchange_version != MIGRATION_EXCHANGE_VERSION {
            return Err(unavailable());
        }
        validate_declaration(&announcement.declaration).map_err(|_| unavailable())?;
        let owner = announcement.declaration.owner.as_str().to_owned();
        Ok(Self {
            proxy,
            owner,
            declaration: announcement.declaration,
        })
    }

    /// The module-owned declaration received during discovery.
    #[must_use]
    pub fn declaration(&self) -> &SchemaDeclaration {
        &self.declaration
    }
}

impl MigrationTransport for MigrationModuleClient {
    fn exchange<'a>(
        &'a self,
        owner: &'a str,
        request: MigrationExchangeRequest,
    ) -> tinystoragedrivers_core::schema::delivery::TransformFuture<'a, MigrationExchangeReply>
    {
        Box::pin(async move {
            if owner != self.owner {
                return Err(unavailable());
            }
            self.proxy
                .call_confidential(EXCHANGE_MEMBER, (MIGRATION_EXCHANGE_VERSION, request))
                .await
                .map_err(|_| unavailable())
        })
    }
}

struct MigrationService {
    trusted_host: TrustedHost,
    declaration: SchemaDeclaration,
    server: MigrationExchangeServer,
    // Dropping the service stops only the reaper; the shared SDK connection
    // remains owned by the TinyBus module runtime.
    _cleanup: tinystoragedrivers_core::schema::delivery::CleanupGuard,
    interface: InterfaceName,
    members: Vec<MemberName>,
}

#[async_trait]
impl Interface for MigrationService {
    fn name(&self) -> InterfaceName {
        self.interface.clone()
    }

    fn members(&self) -> Vec<MemberName> {
        self.members.clone()
    }

    fn requires_confidential(&self, _: &MemberName) -> bool {
        true
    }

    async fn call(&self, _: &MemberName, _: Value) -> tinybus::Result<Value> {
        Err(tinybus::Error::failed(
            "authenticated migration host required",
        ))
    }

    async fn call_with_context(
        &self,
        member: &MemberName,
        args: Value,
        context: &CallContext,
    ) -> tinybus::Result<Value> {
        let peer = self.authenticated_peer(context)?;
        dispatch_authenticated(member, args, &peer, &self.declaration, &self.server).await
    }
}

async fn dispatch_authenticated(
    member: &MemberName,
    args: Value,
    peer: &PeerIdentity,
    declaration: &SchemaDeclaration,
    server: &MigrationExchangeServer,
) -> tinybus::Result<Value> {
    match member.as_str() {
        GET_SCHEMA_MEMBER => {
            if !args.is_null() && args.as_array().is_none_or(|items| !items.is_empty()) {
                return Err(tinybus::Error::failed("invalid migration schema request"));
            }
            encode(SchemaAnnouncement {
                exchange_version: MIGRATION_EXCHANGE_VERSION,
                declaration: declaration.clone(),
            })
        }
        EXCHANGE_MEMBER => {
            let (version, request): (u32, MigrationExchangeRequest) = decode(args)?;
            if version != MIGRATION_EXCHANGE_VERSION {
                return Err(tinybus::Error::failed(
                    "unsupported migration exchange version",
                ));
            }
            encode(server.handle(peer, request).await)
        }
        _ => Err(tinybus::Error::failed("unknown migration member")),
    }
}

impl MigrationService {
    fn authenticated_peer(&self, context: &CallContext) -> tinybus::Result<PeerIdentity> {
        if !self.trusted_host.authorizes(context) {
            return Err(tinybus::Error::failed("migration authority unavailable"));
        }
        let sender = context
            .authenticated_sender()
            .ok_or_else(|| tinybus::Error::failed("migration authority unavailable"))?;
        Ok(PeerIdentity::new(sender.as_str().to_owned()))
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SchemaAnnouncement {
    exchange_version: u32,
    declaration: SchemaDeclaration,
}

struct MonotonicClock(Instant);

impl MonotonicClock {
    fn new() -> Self {
        Self(Instant::now())
    }
}

impl ExchangeClock for MonotonicClock {
    fn now_ms(&self) -> u64 {
        u64::try_from(self.0.elapsed().as_millis()).unwrap_or(u64::MAX)
    }
}

struct UlidTransferIds;

impl TransferIds for UlidTransferIds {
    fn next_id(&self) -> String {
        ulid::Ulid::new().to_string()
    }
}

fn validate_declaration(declaration: &SchemaDeclaration) -> tinybus::Result<()> {
    Registry::compose_with_declarations(
        std::iter::empty::<Schema>(),
        std::iter::once(declaration.clone()),
    )
    .map(|_| ())
    .map_err(|_| tinybus::Error::failed("invalid migration schema declaration"))
}

fn decode<T: DeserializeOwned>(value: Value) -> tinybus::Result<T> {
    serde_json::from_value(value).map_err(|_| tinybus::Error::failed("invalid migration request"))
}

fn encode<T: Serialize>(value: T) -> tinybus::Result<Value> {
    serde_json::to_value(value)
        .map_err(|_| tinybus::Error::failed("migration response serialization failed"))
}

fn unavailable() -> StorageError {
    StorageError::unavailable("migration delivery unavailable")
}

#[cfg(test)]
#[path = "migration_unit_tests.rs"]
mod tests;
