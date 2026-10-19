//! Owner validation and the `Schema` builder.

use super::*;
use crate::ErrorKind;

#[test]
fn accepts_owners_within_the_grammar() {
    for ok in ["cron", "ab", "a1_b2", &"a".repeat(24)] {
        assert_eq!(Owner::new(ok).unwrap().as_str(), ok);
    }
}

#[test]
fn rejects_owners_outside_the_grammar() {
    for bad in ["", "a", "A1", "1ab", "a-b", "a.b", "ab c", &"a".repeat(25)] {
        let error = Owner::new(bad).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput, "{bad}");
        assert!(error.message().contains("owner"), "{bad}");
    }
}

#[test]
fn reserves_the_tsd_owner_for_the_crate() {
    let error = Owner::new("tsd").unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert!(error.message().contains("reserved"));
}

#[test]
fn builds_a_schema_item_by_item() {
    let schema = Schema::new(Owner::new("shop").unwrap())
        .collection("shop.orders", 2, ScopeClass::Tenant)
        .collection_with(
            1,
            CollectionSpec::new("shop.carts").ttl("until"),
            ScopeClass::Agent,
        )
        .stream("shop.audit", ScopeClass::Module)
        .blobs("shop/files/", ScopeClass::Tenant)
        .secrets("shop/keys/", ScopeClass::Cluster)
        .step(Step::new("a", "shop.orders", 1, 2, |_| Ok(Vec::new())).source("orders_v1"));
    assert_eq!(schema.owner.as_str(), "shop");
    let names: Vec<&str> = schema.items.iter().map(Item::name).collect();
    assert_eq!(
        names,
        [
            "shop.orders",
            "shop.carts",
            "shop.audit",
            "shop/files/",
            "shop/keys/"
        ]
    );
    assert_eq!(schema.steps.len(), 1);
    assert_eq!(schema.steps[0].source, Some("orders_v1"));
    assert!(format!("{:?}", schema.steps[0]).contains("shop.orders"));
    assert_eq!(
        TargetRecord::new("c", "i", serde_json::json!({})).created_ms,
        None
    );
}

#[test]
fn a_stream_item_carries_its_seal_policy() {
    use crate::schema::{Item, Owner, Schema, ScopeClass};
    use crate::stream::{SealPolicy, SealWhen};
    let policy = SealPolicy::new(SealWhen::OnFinish).retaining_ms(30 * 86_400_000);
    let schema = Schema::new(Owner::new("harness").unwrap())
        .stream_with("harness.journal", ScopeClass::Agent, policy)
        .stream("harness.audit", ScopeClass::Agent);
    let seals: Vec<_> = schema
        .items
        .iter()
        .map(|item| match item {
            Item::Stream { seal, .. } => *seal,
            _ => unreachable!(),
        })
        .collect();
    assert_eq!(seals, vec![policy, SealPolicy::default()]);
}
