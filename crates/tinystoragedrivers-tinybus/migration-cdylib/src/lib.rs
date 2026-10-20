//! Compiled owner fixture for the TSD migration `TinyBus` adapter.
#![allow(
    missing_docs,
    reason = "The cdylib exports are a private integration fixture."
)]

use std::sync::Arc;

use tinybus::{ObjectPath, Result, WeakConnection};
use tinybus_module::ModuleSetupContext;
use tinystoragedrivers_tinybus::migration::{
    CollectionSpec, Expiry, Item, MigrationPayload, MigrationPayloadHandler, MigrationPayloadReply,
    Owner, SchemaDeclaration, ScopeClass, StepDeclaration, TargetRecord, TransformDescriptor,
    TransformError, TransformErrorKind, TransformKind, serve_module_migration,
};

struct Fixture {
    connection: WeakConnection,
}

#[tinybus::interface(name = "ai.tinyhumans.test.MigrationFixture")]
impl Fixture {
    async fn ping(&self) -> Result<&'static str> {
        let connection = self
            .connection
            .upgrade()
            .ok_or_else(|| tinybus::Error::failed("fixture connection is not connected"))?;
        let unique_name = connection
            .unique_name()
            .ok_or_else(|| tinybus::Error::failed("fixture connection is not connected"))?;
        let owner = connection.name_owner(unique_name.as_str()).await?;
        if owner.as_ref() == Some(&unique_name) {
            Ok("fixture-alive")
        } else {
            Err(tinybus::Error::failed(
                "fixture connection no longer owns its unique name",
            ))
        }
    }
}

struct Handler;

impl MigrationPayloadHandler for Handler {
    fn transform(&self, payload: MigrationPayload) -> MigrationPayloadReply {
        let MigrationPayload::Documents(request) = payload else {
            return MigrationPayloadReply::Documents(Err(TransformError {
                kind: TransformErrorKind::UnsupportedOperation,
                message: "unsupported fixture migration payload".to_owned(),
            }));
        };
        MigrationPayloadReply::Documents(Ok(request
            .records
            .into_iter()
            .map(|record| TargetRecord {
                collection: "fixture.records".to_owned(),
                id: record.id,
                body: serde_json::json!({"legacy":record.body,"version":2}),
                created_ms: None,
                expires: Expiry::Carry,
            })
            .collect()))
    }
}

fn declaration() -> Result<SchemaDeclaration> {
    Ok(SchemaDeclaration {
        owner: Owner::new("fixture")
            .map_err(|_| tinybus::Error::failed("fixture schema owner is invalid"))?,
        items: vec![Item::Collection {
            name: "fixture.records".to_owned(),
            version: 2,
            spec: CollectionSpec::new("fixture.records"),
            scope: ScopeClass::Module,
        }],
        steps: vec![StepDeclaration {
            id: "records_v2".to_owned(),
            item: "fixture.records".to_owned(),
            from: 1,
            to: 2,
            source: Some("legacy_fixture_records".to_owned()),
            transform: TransformDescriptor {
                operation: "records_v2".to_owned(),
                version: 1,
                kind: TransformKind::Documents,
            },
        }],
    })
}

async fn setup(context: ModuleSetupContext) -> Result<()> {
    serve_module_migration(&context, declaration()?, Arc::new(Handler)).await?;
    context
        .connection()
        .serve_at(
            ObjectPath::new("/ai/tinyhumans/test/MigrationFixture")?,
            Fixture {
                connection: context.connection().downgrade(),
            },
        )
        .await?;
    context
        .connection()
        .request_name("ai.tinyhumans.test.MigrationFixture")
        .await?;
    Ok(())
}

tinybus_module::module_export! {
    setup_with_context = setup,
    worker_threads = 1,
    provides = [
        "ai.tinyhumans.test.MigrationFixture",
        "org.tinystoragedrivers.Migration",
    ],
    methods = ["Ping", "GetSchema", "Exchange"],
    signals = [],
    requires = [],
    optional = [],
    lazy = false,
}
