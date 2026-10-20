//! Real broker and compiled-module coverage for TSD migration delivery.
use std::{
    error::Error,
    path::{Path, PathBuf},
    sync::Arc,
};

use async_trait::async_trait;
use serde_json::{Value, json};
use tinybus::{
    Connection, Proxy, broker::Broker, module::ModuleHost, transport::memory::MemoryTransport,
};
use tinystoragedrivers_bus::{CollectionSpec as BusCollectionSpec, StorageNeeds};
use tinystoragedrivers_busproxy::{
    AuditEvent, AuditSink, Environment, GovernedEndpointLimits, GovernedStorage, GrantPolicy,
    HostContext, HostFailure, HostStorageContext, ResolvedStorage, StorageGrant,
};
use tinystoragedrivers_client::StorageServer;
use tinystoragedrivers_core::{
    MemoryStorage, Precondition, Scope, StorageBackend,
    schema::Registry,
    schema::delivery::{MigrationDeliveryClient, MigrationExchangeRequest, MigrationTransport},
};
use tinystoragedrivers_secrets::{DerivedKeys, DocumentSecrets};

type TestResult<T> = Result<T, Box<dyn Error>>;

use crate::migration::{
    EXCHANGE_MEMBER, GET_SCHEMA_MEMBER, MIGRATION_INTERFACE, MIGRATION_OBJECT_PATH,
    MigrationModuleClient,
};

fn stage_allowlisted_cdylib(source: &Path) -> TestResult<(tempfile::TempDir, PathBuf)> {
    let parent = source
        .parent()
        .ok_or_else(|| std::io::Error::other("fixture has no parent directory"))?;
    let source_name = source
        .file_name()
        .ok_or_else(|| std::io::Error::other("fixture has no file name"))?;
    let directory = tempfile::tempdir_in(parent)?;
    let staged = directory.path().join(source_name);
    std::fs::copy(source, &staged)?;
    let digest = tinybus::module::sha256_file(&staged)?;
    let staged_name = staged
        .file_name()
        .ok_or_else(|| std::io::Error::other("staged fixture has no file name"))?;
    std::fs::write(
        directory.path().join("modules.toml"),
        format!("{:?} = {:?}\n", staged_name.to_string_lossy(), digest),
    )?;
    Ok((directory, staged))
}

#[derive(Clone, Copy)]
struct Profile(&'static str);
impl HostContext for Profile {}

struct EnvironmentForTest {
    backend: Arc<MemoryStorage>,
}

#[async_trait]
impl Environment<Profile> for EnvironmentForTest {
    async fn resolve(
        &self,
        context: &Profile,
        _module: &tinybus::module::ModuleId,
    ) -> Result<ResolvedStorage, HostFailure> {
        let scope = Scope::new(context.0).map_err(|_| HostFailure::Denied("invalid-test-scope"))?;
        let storage = self
            .backend
            .for_scope(&scope)
            .map_err(|_| HostFailure::Denied("test-storage-unavailable"))?;
        let collections = [BusCollectionSpec {
            name: "shared.records".to_owned(),
            indexes: Vec::new(),
            ttl_field: None,
            search: None,
        }];
        let blob_prefixes = ["canary/"];
        let stream_prefixes = ["events/"];
        let secret_names = ["provider.key"];
        let needs = StorageNeeds {
            collections: &collections,
            blob_prefixes: &blob_prefixes,
            stream_prefixes: &stream_prefixes,
            secret_prefixes: &[],
        }
        .with_exact_secret_names(&secret_names);
        let grant = StorageGrant::from_needs_with_exact_secrets(
            needs,
            &GrantPolicy {
                writable: true,
                allow_secrets: true,
                ..GrantPolicy::default()
            },
        )
        .map_err(|_| HostFailure::Denied("invalid-test-grant"))?;
        let keys = Arc::new(
            DerivedKeys::from_hex(
                "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
            )
            .map_err(|_| HostFailure::Denied("test-key-unavailable"))?,
        );
        let secrets = Arc::new(DocumentSecrets::new(&storage, keys));
        Ok(ResolvedStorage {
            backend_identity: "migration-fixture-memory".to_owned(),
            binding_epoch: 1,
            canonical_scope: context.0.to_owned(),
            grant,
            server: Arc::new(StorageServer::new(storage).with_secrets(secrets)),
        })
    }

    async fn admit_write(
        &self,
        _context: &Profile,
        _module: &tinybus::module::ModuleId,
    ) -> Result<(), HostFailure> {
        Ok(())
    }
}

#[derive(Default)]
struct NoAudit;

impl AuditSink for NoAudit {
    fn record(&self, _event: AuditEvent) {}
}

async fn connect_peer(broker: &Broker, controller: bool) -> tinybus::Result<Connection> {
    let (broker_transport, client_transport) = MemoryTransport::pair();
    if controller {
        broker.attach_module_controller(Arc::new(broker_transport))?;
    } else {
        broker.attach(Arc::new(broker_transport));
    }
    Connection::connect(Box::new(client_transport)).await
}

struct CompiledFixture {
    _staging: tempfile::TempDir,
    broker: Broker,
    controller: Connection,
    outsider: Connection,
    host: ModuleHost,
    module_peer: tinybus::BusName,
}

fn test_failure(message: &'static str) -> Box<dyn Error> {
    std::io::Error::other(message).into()
}

async fn load_compiled_fixture(
    source: &Path,
) -> TestResult<(CompiledFixture, MigrationModuleClient)> {
    let (staging, staged) = stage_allowlisted_cdylib(source)?;
    let unselected_host = ModuleHost::new(Broker::new());
    let unselected_info = unselected_host.load_file(&staged)?;
    assert!(
        unselected_host
            .ensure_ready(unselected_info.manifest.bus_name.as_str())
            .await
            .is_err(),
        "migration-required module setup fails closed without a loader-issued host binding"
    );
    drop(unselected_host);

    let broker = Broker::new_restricted_module_control();
    let controller = connect_peer(&broker, true).await?;
    let outsider = connect_peer(&broker, false).await?;
    let host = ModuleHost::new(broker.clone());
    let info = host.load_file(&staged)?;
    host.ensure_ready(info.manifest.bus_name.as_str()).await?;
    let attestation = controller
        .attestation(info.manifest.bus_name.clone())
        .await?;
    assert!(
        attestation.is_some(),
        "allowlisted module must accept confidential calls"
    );
    let module_peer = controller
        .name_owner(info.manifest.bus_name.as_str())
        .await?
        .ok_or_else(|| test_failure("compiled module does not own its bus name"))?;
    let module = MigrationModuleClient::discover(&controller, &module_peer).await?;
    Ok((
        CompiledFixture {
            _staging: staging,
            broker,
            controller,
            outsider,
            host,
            module_peer,
        },
        module,
    ))
}

async fn verify_selected_controller_protocol(
    fixture: &CompiledFixture,
    module: &MigrationModuleClient,
) -> TestResult<()> {
    let raw_schema = fixture
        .controller
        .proxy(
            fixture.module_peer.as_str(),
            MIGRATION_OBJECT_PATH,
            MIGRATION_INTERFACE,
        )?
        .call_confidential::<Value>(GET_SCHEMA_MEMBER, ())
        .await;
    assert!(
        raw_schema.is_ok(),
        "raw selected schema call failed: {raw_schema:?}"
    );

    let malformed_schema = fixture
        .controller
        .proxy(
            fixture.module_peer.as_str(),
            MIGRATION_OBJECT_PATH,
            MIGRATION_INTERFACE,
        )?
        .call_confidential::<Value>(GET_SCHEMA_MEMBER, json!(1))
        .await;
    let Err(malformed_schema) = malformed_schema else {
        return Err(test_failure(
            "authorized malformed schema request unexpectedly succeeded",
        ));
    };
    assert!(
        malformed_schema
            .to_string()
            .contains("invalid migration schema request")
    );

    let unsupported_version = fixture
        .controller
        .proxy(
            fixture.module_peer.as_str(),
            MIGRATION_OBJECT_PATH,
            MIGRATION_INTERFACE,
        )?
        .call_confidential::<Value>(
            EXCHANGE_MEMBER,
            (0_u32, MigrationExchangeRequest::Begin { request_len: 1 }),
        )
        .await;
    let Err(unsupported_version) = unsupported_version else {
        return Err(test_failure(
            "unsupported migration version unexpectedly succeeded",
        ));
    };
    assert!(
        unsupported_version
            .to_string()
            .contains("unsupported migration exchange version")
    );
    assert!(
        module
            .exchange(
                "another-owner",
                MigrationExchangeRequest::Begin { request_len: 1 }
            )
            .await
            .is_err(),
        "the client must not forward owner-mismatched requests"
    );
    Ok(())
}

async fn migrate_fixture_record(module: MigrationModuleClient) -> TestResult<()> {
    let declaration = module.declaration().clone();
    let backend = MemoryStorage::new();
    let registry =
        Registry::compose_with_declarations(std::iter::empty(), std::iter::once(declaration))?;
    registry.prepare(&backend).await?;
    let migration_scope = Scope::new("migration-proof")?;
    let scope = backend.for_scope(&migration_scope)?;
    scope
        .documents()
        .put(
            "legacy_fixture_records",
            "row-1",
            json!({"value":"legacy"}),
            Precondition::Absent,
        )
        .await?;
    let executor = MigrationDeliveryClient::new(Arc::new(module));
    registry
        .migrate_with_executor(
            &scope,
            &tinystoragedrivers_core::schema::MigrateOptions::new(),
            &executor,
        )
        .await?;
    registry.check_ready_in(&migration_scope, "fixture.records")?;
    let migrated = scope
        .documents()
        .get("fixture.records", "row-1")
        .await?
        .ok_or_else(|| test_failure("migration did not write the target record"))?;
    assert_eq!(
        migrated.doc,
        json!({"legacy":{"value":"legacy"},"version":2})
    );
    assert!(
        scope
            .documents()
            .get("legacy_fixture_records", "row-1")
            .await?
            .is_none(),
        "migration must remove the source record"
    );
    Ok(())
}

async fn verify_other_senders_denied(fixture: &CompiledFixture) -> TestResult<Proxy> {
    let outsider_migration = fixture.outsider.proxy(
        fixture.module_peer.as_str(),
        MIGRATION_OBJECT_PATH,
        MIGRATION_INTERFACE,
    )?;
    let malformed = outsider_migration
        .call_confidential::<Value>(EXCHANGE_MEMBER, json!(17))
        .await;
    let Err(malformed) = malformed else {
        return Err(test_failure(
            "unauthorized malformed exchange unexpectedly succeeded",
        ));
    };
    assert!(
        malformed
            .to_string()
            .contains("migration authority unavailable"),
        "the routed outsider must be denied before malformed payload decoding: {malformed}"
    );
    let plain_controller = fixture.controller.proxy(
        fixture.module_peer.as_str(),
        MIGRATION_OBJECT_PATH,
        MIGRATION_INTERFACE,
    )?;
    assert!(
        plain_controller
            .call::<Value>(GET_SCHEMA_MEMBER, ())
            .await
            .is_err(),
        "schema discovery requires confidential routing"
    );
    let outsider_valid = outsider_migration
        .call_confidential::<Value>(GET_SCHEMA_MEMBER, ())
        .await;
    let Err(outsider_valid) = outsider_valid else {
        return Err(test_failure(
            "outsider schema discovery unexpectedly succeeded",
        ));
    };
    assert!(
        outsider_valid
            .to_string()
            .contains("migration authority unavailable")
    );
    Ok(outsider_migration)
}

#[cfg(not(windows))]
#[tokio::test]
#[ignore = "requires TSD_MIGRATION_TEST_MODULE to point at the built consumer cdylib"]
async fn compiled_owner_migrates_before_storage_lease_and_rejects_other_senders() -> TestResult<()>
{
    let source = std::env::var_os("TSD_MIGRATION_TEST_MODULE")
        .map(PathBuf::from)
        .ok_or_else(|| test_failure("TSD_MIGRATION_TEST_MODULE is not set"))?;
    let (fixture, module) = load_compiled_fixture(&source).await?;
    verify_selected_controller_protocol(&fixture, &module).await?;
    migrate_fixture_record(module).await?;
    let outsider_migration = verify_other_senders_denied(&fixture).await?;

    let storage_endpoint = GovernedStorage::new(
        Arc::new(EnvironmentForTest {
            backend: Arc::new(MemoryStorage::new()),
        }),
        Arc::new(NoAudit),
        GovernedEndpointLimits::default(),
    );
    let registration = crate::attach_storage(
        &fixture.host,
        &fixture.controller,
        storage_endpoint,
        tokio::runtime::Handle::current(),
    )
    .await
    .map_err(|_| test_failure("storage lease registration failed after migration"))?;
    let (_lease, _descriptor) = registration
        .register_lease(
            HostStorageContext::new(Profile("post-migration")),
            fixture.module_peer.clone(),
        )
        .await
        .map_err(|_| test_failure("ordinary storage lease publication failed"))?;
    let consumer = fixture.controller.proxy(
        fixture.module_peer.as_str(),
        "/ai/tinyhumans/test/MigrationFixture",
        "ai.tinyhumans.test.MigrationFixture",
    )?;
    let ping: String = consumer.call("Ping", ()).await?;
    assert_eq!(ping, "fixture-alive");

    registration.shutdown().await?;
    let ping_after_storage_shutdown: String = consumer.call("Ping", ()).await?;
    assert_eq!(ping_after_storage_shutdown, "fixture-alive");

    fixture.controller.close().await?;
    assert!(
        outsider_migration
            .call_confidential::<Value>(GET_SCHEMA_MEMBER, ())
            .await
            .is_err(),
        "a later caller cannot inherit the disconnected controller binding"
    );
    let (replacement, _replacement_client) = MemoryTransport::pair();
    assert!(
        fixture
            .broker
            .attach_module_controller(Arc::new(replacement))
            .is_err(),
        "a selected controller cannot be replaced on the same broker"
    );
    Ok(())
}
