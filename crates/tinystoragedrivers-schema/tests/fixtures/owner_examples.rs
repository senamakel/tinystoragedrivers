// Independently constructed DTO examples. Expected wire values live in JSON,
// never derived from these builders. Provenance and mapping: fixtures/README.md.
use ScopeClass::{Module, Tenant};
use TransformKind::{Blob, Documents, Stream};
use tinystoragedrivers_schema::*;

fn collection(name: &str, scope: ScopeClass, indexes: &[&str]) -> Item {
    let spec = indexes
        .iter()
        .fold(CollectionSpec::new(name), |spec, field| {
            spec.index(IndexSpec::new(*field, [*field]))
        });
    Item::Collection {
        name: name.into(),
        version: 2,
        spec,
        scope,
    }
}

fn stream(name: &str, when: SealWhen) -> Item {
    Item::Stream {
        name: name.into(),
        scope: ScopeClass::Module,
        seal: SealPolicy::new(when),
    }
}

fn step(id: &str, item: &str, source: &str, kind: TransformKind) -> StepDeclaration {
    StepDeclaration {
        id: id.into(),
        item: item.into(),
        from: 1,
        to: 2,
        source: Some(source.into()),
        transform: TransformDescriptor {
            operation: id.into(),
            kind,
            version: 1,
        },
    }
}

#[expect(
    clippy::unwrap_used,
    reason = "constant valid owners pinned by frozen fixtures"
)]
fn declaration(owner: &str, items: Vec<Item>, steps: Vec<StepDeclaration>) -> SchemaDeclaration {
    SchemaDeclaration {
        owner: Owner::new(owner).unwrap(),
        items,
        steps,
    }
}

pub(super) fn all() -> Vec<SchemaDeclaration> {
    vec![
        juice(),
        connectors(),
        mcp(),
        channels(),
        wallet(),
        memes(),
        box_owner(),
    ]
}

fn juice() -> SchemaDeclaration {
    declaration(
        "juice",
        vec![Item::Blobs {
            prefix: "juice/ccr/".into(),
            scope: Module,
        }],
        vec![step("ccr_prefix_v2", "juice/ccr/", "tinyjuice_ccr/", Blob)],
    )
}

fn connectors() -> SchemaDeclaration {
    declaration(
        "connectors",
        vec![
            collection("connectors.user_scopes", Module, &[]),
            stream("connectors.triggers", SealWhen::Idle(172_800_000)),
        ],
        vec![
            step(
                "user_scopes_unwrap_value",
                "connectors.user_scopes",
                "composio-user-scopes",
                Documents,
            ),
            step(
                "triggers_rename",
                "connectors.triggers",
                "triggers/",
                Stream,
            ),
        ],
    )
}

fn mcp() -> SchemaDeclaration {
    declaration(
        "mcp",
        vec![
            collection("mcp.servers", Module, &["qualified_name", "installed_ms"]),
            collection("mcp.server_names", Module, &[]),
            collection("mcp.registry_cache", Module, &["cache_key"]),
            collection("mcp.tool_cache", Module, &[]),
            collection("mcp.registry_index", Module, &["source"]),
            collection("mcp.registry_index_state", Module, &[]),
            stream("mcp.audit", SealWhen::Never),
        ],
        vec![
            step("servers_v2", "mcp.servers", "mcp_servers", Documents),
            step(
                "server_names_v2",
                "mcp.server_names",
                "mcp_server_names",
                Documents,
            ),
            step(
                "registry_cache_v2",
                "mcp.registry_cache",
                "mcp_registry_cache",
                Documents,
            ),
            step(
                "tool_cache_v2",
                "mcp.tool_cache",
                "mcp_tool_cache",
                Documents,
            ),
            step(
                "registry_index_v2",
                "mcp.registry_index",
                "mcp_registry_index",
                Documents,
            ),
            step(
                "registry_index_state_v2",
                "mcp.registry_index_state",
                "mcp_registry_index_state",
                Documents,
            ),
            step("audit_v2", "mcp.audit", "mcp_audit", Stream),
        ],
    )
}

fn channels() -> SchemaDeclaration {
    declaration(
        "channels",
        channels_items(),
        vec![
            step(
                "delivery-pending-v2",
                "channels.delivery_pending",
                "channels_delivery_pending",
                Documents,
            ),
            step(
                "delivery-failed-v2",
                "channels.delivery_failed",
                "channels_delivery_failed",
                Documents,
            ),
            step(
                "delivery-results-v2",
                "channels.delivery_results",
                "channels_delivery_results",
                Documents,
            ),
            step(
                "remote-bindings-v2",
                "channels.remote_bindings",
                "channel_remote_bindings",
                Documents,
            ),
            step(
                "remote-busy-v2",
                "channels.remote_busy",
                "channel_remote_busy",
                Documents,
            ),
            step(
                "remote-meta-v2",
                "channels.remote_meta",
                "channel_remote_meta",
                Documents,
            ),
            step(
                "whatsapp-device-v2",
                "channels.whatsapp_device",
                "whatsapp_device",
                Documents,
            ),
            step(
                "whatsapp-identity-v2",
                "channels.whatsapp_identity",
                "whatsapp_identity",
                Documents,
            ),
            step(
                "whatsapp-session-v2",
                "channels.whatsapp_session",
                "whatsapp_session",
                Documents,
            ),
            step(
                "whatsapp-prekey-v2",
                "channels.whatsapp_prekey",
                "whatsapp_prekey",
                Documents,
            ),
            step(
                "whatsapp-signed-prekey-v2",
                "channels.whatsapp_signed_prekey",
                "whatsapp_signed_prekey",
                Documents,
            ),
            step(
                "whatsapp-sender-key-v2",
                "channels.whatsapp_sender_key",
                "whatsapp_sender_key",
                Documents,
            ),
            step(
                "whatsapp-sync-key-v2",
                "channels.whatsapp_sync_key",
                "whatsapp_sync_key",
                Documents,
            ),
            step(
                "whatsapp-lid-mapping-v2",
                "channels.whatsapp_lid_mapping",
                "whatsapp_lid_mapping",
                Documents,
            ),
        ],
    )
}

fn wallet() -> SchemaDeclaration {
    declaration(
        "wallet",
        vec![
            collection("wallet.budgets", Module, &[]),
            stream("wallet.payments", SealWhen::Never),
        ],
        vec![
            step("budgets_v2", "wallet.budgets", "x402_budget", Documents),
            step("payments_v2", "wallet.payments", "x402.payments", Stream),
        ],
    )
}

fn memes() -> SchemaDeclaration {
    declaration(
        "memes",
        vec![collection("memes.state", Tenant, &[])],
        vec![step(
            "state_v2",
            "memes.state",
            "tinymemes_documents",
            Documents,
        )],
    )
}

fn box_owner() -> SchemaDeclaration {
    declaration(
        "box",
        vec![
            collection("box.boxes", Module, &[]),
            collection("box.jail_index", Module, &[]),
        ],
        vec![
            step("rename_boxes", "box.boxes", "tinybox_boxes", Documents),
            step(
                "rename_jail_index",
                "box.jail_index",
                "tinybox_jail_index",
                Documents,
            ),
        ],
    )
}

fn channels_items() -> Vec<Item> {
    vec![
        collection("channels.delivery_pending", Module, &[]),
        collection("channels.delivery_failed", Module, &[]),
        collection("channels.delivery_results", Module, &[]),
        collection("channels.remote_bindings", Module, &[]),
        collection("channels.remote_busy", Module, &[]),
        collection("channels.remote_meta", Module, &[]),
        collection("channels.whatsapp_device", Module, &[]),
        collection("channels.whatsapp_identity", Module, &[]),
        collection("channels.whatsapp_session", Module, &[]),
        collection("channels.whatsapp_prekey", Module, &[]),
        collection("channels.whatsapp_signed_prekey", Module, &[]),
        collection("channels.whatsapp_sender_key", Module, &[]),
        collection("channels.whatsapp_sync_key", Module, &[]),
        collection("channels.whatsapp_lid_mapping", Module, &[]),
    ]
}
