//! Scope retrieval across every stored item kind.
use super::*;

#[test]
fn each_item_returns_its_declared_scope_for_every_scope_class() {
    for scope in [
        ScopeClass::Tenant,
        ScopeClass::Agent,
        ScopeClass::Module,
        ScopeClass::Cluster,
    ] {
        let items = [
            Item::Collection {
                name: "orders.rows".into(),
                version: 1,
                spec: CollectionSpec::new("orders.rows"),
                scope,
            },
            Item::Stream {
                name: "orders.audit".into(),
                scope,
                seal: SealPolicy::default(),
            },
            Item::Blobs {
                prefix: "orders/files/".into(),
                scope,
            },
            Item::Secrets {
                prefix: "orders/keys/".into(),
                scope,
            },
        ];
        for item in items {
            assert_eq!(item.scope(), scope, "scope of {}", item.name());
        }
    }
}
