//! Focused protocol tests for authenticated migration dispatch.
use super::*;
use async_trait::async_trait;
use std::sync::atomic::{AtomicU64, Ordering};
use tinybus::{
    Connection, Interface, InterfaceName, MemberName, ObjectPath, transport::memory::MemoryBus,
};
use tinystoragedrivers_core::schema::delivery::{
    MigrationExchangeReply, MigrationPayload, MigrationPayloadReply,
};

struct Handler;

impl MigrationPayloadHandler for Handler {
    fn transform(&self, _: MigrationPayload) -> MigrationPayloadReply {
        MigrationPayloadReply::BlobTarget(Ok(None))
    }
}

#[derive(Default)]
struct Clock(AtomicU64);

impl ExchangeClock for Clock {
    fn now_ms(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}

#[derive(Default)]
struct Ids(AtomicU64);

impl TransferIds for Ids {
    fn next_id(&self) -> String {
        format!("transfer-{}", self.0.fetch_add(1, Ordering::SeqCst))
    }
}

fn declaration() -> SchemaDeclaration {
    SchemaDeclaration {
        owner: Owner::new("fixture").unwrap(),
        items: vec![Item::Collection {
            name: "fixture.records".to_owned(),
            version: 1,
            spec: CollectionSpec::new("fixture.records"),
            scope: ScopeClass::Module,
        }],
        steps: Vec::new(),
    }
}

fn server() -> MigrationExchangeServer {
    MigrationExchangeServer::new(
        Arc::new(Handler),
        Arc::new(Clock::default()),
        Arc::new(Ids::default()),
        DEFAULT_MAX_IN_FLIGHT,
    )
}

const TEST_MODULE_NAME: &str = "org.tinystoragedrivers.testmigration";

mod client_fixture_module {
    async fn setup(_: tinybus::Connection) -> tinybus::Result<()> {
        Ok(())
    }

    tinybus_module::module_export_static! {
        setup = setup,
        worker_threads = 1,
        provides = ["org.tinystoragedrivers.Migration"],
        methods = ["GetSchema", "Exchange"],
        signals = [],
        requires = [],
        optional = [],
        lazy = false,
    }
}

struct ClientFixture {
    // This linked fixture is only for host-client protocol tests. Sender
    // authorization is exercised by the separately compiled real adapter test.
    announcement: Value,
    reply: Value,
    interface: InterfaceName,
    members: Vec<MemberName>,
}

#[async_trait]
impl Interface for ClientFixture {
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
        Err(tinybus::Error::failed("confidential call required"))
    }

    async fn call_with_context(
        &self,
        member: &MemberName,
        _: Value,
        _: &CallContext,
    ) -> tinybus::Result<Value> {
        match member.as_str() {
            GET_SCHEMA_MEMBER => Ok(self.announcement.clone()),
            EXCHANGE_MEMBER => Ok(self.reply.clone()),
            _ => Err(tinybus::Error::failed("unknown fixture member")),
        }
    }
}

async fn client_fixture(
    declaration: SchemaDeclaration,
    exchange_version: u32,
) -> (
    Connection,
    BusName,
    tinybus::module::ModuleHost,
    tokio::task::JoinHandle<tinybus::Result<()>>,
) {
    let bus = MemoryBus::new();
    let broker = tinybus::broker::Broker::new();
    let broker_task = broker.clone().spawn(bus.clone());
    let host = tinybus::module::ModuleHost::new(broker);
    let fixture = ClientFixture {
        announcement: serde_json::to_value(SchemaAnnouncement {
            exchange_version,
            declaration,
        })
        .unwrap(),
        reply: serde_json::to_value(MigrationExchangeReply::Begun {
            transfer: "fixture-transfer".to_owned(),
        })
        .unwrap(),
        interface: InterfaceName::new(MIGRATION_INTERFACE).unwrap(),
        members: vec![
            MemberName::new(GET_SCHEMA_MEMBER).unwrap(),
            MemberName::new(EXCHANGE_MEMBER).unwrap(),
        ],
    };
    let mut export = client_fixture_module::linked_module().unwrap();
    let module_id = "migration-client-fixture";
    export.descriptor.module_name = [0; 64];
    export.descriptor.module_name[..module_id.len()].copy_from_slice(module_id.as_bytes());
    export.manifest.module.name = module_id.to_owned();
    export.manifest.bus_name = TEST_MODULE_NAME.parse().unwrap();
    export.manifest.object_path = MIGRATION_OBJECT_PATH.parse().unwrap();
    host.attach_linked_with_setup(export, Value::Null, move |connection| async move {
        connection
            .serve_at(ObjectPath::new(MIGRATION_OBJECT_PATH)?, fixture)
            .await?;
        connection.request_name(TEST_MODULE_NAME).await
    })
    .await
    .unwrap();
    let client = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let module_peer = client
        .name_owner(TEST_MODULE_NAME)
        .await
        .unwrap()
        .expect("fixture module owns its published name");
    (client, module_peer, host, broker_task)
}

#[tokio::test]
async fn authenticated_dispatch_validates_discovery_and_exchange_before_handling() {
    let declaration = declaration();
    let server = server();
    let peer = PeerIdentity::new(":1.4");
    let get_schema = MemberName::new(GET_SCHEMA_MEMBER).unwrap();
    let announcement = dispatch_authenticated(
        &get_schema,
        Value::Array(Vec::new()),
        &peer,
        &declaration,
        &server,
    )
    .await
    .unwrap();
    let announcement: SchemaAnnouncement = serde_json::from_value(announcement).unwrap();
    assert_eq!(announcement.exchange_version, MIGRATION_EXCHANGE_VERSION);
    assert_eq!(announcement.declaration, declaration);

    let bad_schema = dispatch_authenticated(
        &get_schema,
        serde_json::json!(17),
        &peer,
        &declaration,
        &server,
    )
    .await
    .unwrap_err();
    assert!(
        bad_schema
            .to_string()
            .contains("invalid migration schema request")
    );

    let exchange = MemberName::new(EXCHANGE_MEMBER).unwrap();
    let malformed = dispatch_authenticated(
        &exchange,
        serde_json::json!(17),
        &peer,
        &declaration,
        &server,
    )
    .await
    .unwrap_err();
    assert!(malformed.to_string().contains("invalid migration request"));

    let unsupported = dispatch_authenticated(
        &exchange,
        serde_json::to_value((
            MIGRATION_EXCHANGE_VERSION - 1,
            MigrationExchangeRequest::Begin { request_len: 1 },
        ))
        .unwrap(),
        &peer,
        &declaration,
        &server,
    )
    .await
    .unwrap_err();
    assert!(
        unsupported
            .to_string()
            .contains("unsupported migration exchange version")
    );

    let reply = dispatch_authenticated(
        &exchange,
        serde_json::to_value((
            MIGRATION_EXCHANGE_VERSION,
            MigrationExchangeRequest::Begin { request_len: 1 },
        ))
        .unwrap(),
        &peer,
        &declaration,
        &server,
    )
    .await
    .unwrap();
    let reply: MigrationExchangeReply = serde_json::from_value(reply).unwrap();
    assert!(matches!(reply, MigrationExchangeReply::Begun { .. }));

    let unknown = MemberName::new("Unknown").unwrap();
    assert!(
        dispatch_authenticated(&unknown, Value::Null, &peer, &declaration, &server)
            .await
            .unwrap_err()
            .to_string()
            .contains("unknown migration member")
    );
}

#[test]
fn setup_builder_fails_closed_and_schema_validation_rejects_foreign_owner_items() {
    let missing = new_migration_service(None, declaration(), Arc::new(Handler))
        .err()
        .expect("missing trusted binding must fail");
    assert!(
        missing
            .to_string()
            .contains("trusted migration host binding unavailable")
    );

    let mut invalid = declaration();
    invalid.items[0] = Item::Collection {
        name: "other.records".to_owned(),
        version: 1,
        spec: CollectionSpec::new("other.records"),
        scope: ScopeClass::Module,
    };
    assert!(validate_declaration(&invalid).is_err());
}

#[test]
fn exchange_clock_and_ids_are_monotonic_and_fresh() {
    let clock = MonotonicClock::new();
    let before = clock.now_ms();
    let later = clock.now_ms();
    assert!(later >= before);

    let ids = UlidTransferIds;
    let first = ids.next_id();
    let second = ids.next_id();
    assert_eq!(first.len(), 26);
    assert_ne!(first, second);
}

#[tokio::test]
async fn host_client_validates_discovery_and_uses_only_the_declared_owner() {
    let (controller, module_peer, _host, _broker_task) =
        client_fixture(declaration(), MIGRATION_EXCHANGE_VERSION).await;
    let raw = controller
        .proxy(TEST_MODULE_NAME, MIGRATION_OBJECT_PATH, MIGRATION_INTERFACE)
        .unwrap()
        .call_confidential::<Value>(GET_SCHEMA_MEMBER, ())
        .await;
    assert!(raw.is_ok(), "fixture raw schema call failed: {raw:?}");
    let client = MigrationModuleClient::discover(&controller, &module_peer)
        .await
        .unwrap();
    assert_eq!(client.declaration().owner.as_str(), "fixture");
    assert!(format!("{client:?}").contains("MigrationModuleClient"));

    let mismatched_owner = client
        .exchange(
            "another_owner",
            MigrationExchangeRequest::Begin { request_len: 1 },
        )
        .await
        .unwrap_err();
    assert_eq!(
        mismatched_owner.kind(),
        tinystoragedrivers_core::ErrorKind::Unavailable
    );

    let reply = client
        .exchange(
            "fixture",
            MigrationExchangeRequest::Begin { request_len: 1 },
        )
        .await
        .unwrap();
    assert_eq!(
        reply,
        MigrationExchangeReply::Begun {
            transfer: "fixture-transfer".to_owned(),
        }
    );
}

#[tokio::test]
async fn host_client_sanitizes_wrong_version_and_invalid_declaration() {
    let (wrong_version, peer, _host, _broker_task) =
        client_fixture(declaration(), MIGRATION_EXCHANGE_VERSION - 1).await;
    assert_eq!(
        MigrationModuleClient::discover(&wrong_version, &peer)
            .await
            .unwrap_err()
            .kind(),
        tinystoragedrivers_core::ErrorKind::Unavailable
    );

    let mut invalid = declaration();
    invalid.items[0] = Item::Collection {
        name: "other.records".to_owned(),
        version: 1,
        spec: CollectionSpec::new("other.records"),
        scope: ScopeClass::Module,
    };
    let (invalid_declaration, peer, _host, _broker_task) =
        client_fixture(invalid, MIGRATION_EXCHANGE_VERSION).await;
    assert_eq!(
        MigrationModuleClient::discover(&invalid_declaration, &peer)
            .await
            .unwrap_err()
            .kind(),
        tinystoragedrivers_core::ErrorKind::Unavailable
    );
}
