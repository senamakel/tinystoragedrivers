//! Tests for exporting local schema metadata as a wire declaration.
use super::*;
use crate::{Meta, SourceRecord};
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

fn schema_with_all_transform_families() -> Schema {
    Schema::new(Owner::new("sample").expect("valid owner"))
        .collection("sample.records", 2, ScopeClass::Module)
        .stream("sample.events", ScopeClass::Module)
        .blobs("sample/files/", ScopeClass::Module)
        .step(
            Step::new("records_v2", "sample.records", 1, 2, |records| {
                Ok(records
                    .into_iter()
                    .map(|record| TargetRecord::new("sample.records", record.id, record.body))
                    .collect())
            })
            .source("legacy_records"),
        )
        .step(
            Step::stream("events_v2", "sample.events", 1, 2, |name, entries| {
                Ok(Some((format!("sample.events/{name}"), entries)))
            })
            .source_prefix("legacy-events/"),
        )
        .step(
            Step::blobs(
                "files_v2",
                "sample/files/",
                1,
                2,
                |key, content_type, bytes| {
                    Ok(Some((format!("sample/files/{key}"), content_type, bytes)))
                },
            )
            .source_prefix("legacy-files/"),
        )
}

#[test]
fn declaration_copies_schema_metadata_and_derives_transform_families() {
    let schema = schema_with_all_transform_families();
    let declaration =
        declaration_from_schema(&schema, |step| Some((format!("sample.{}", step.id), 1)))
            .expect("every step has a declared operation");

    assert_eq!(declaration.owner, schema.owner);
    assert_eq!(declaration.items, schema.items);
    assert_eq!(declaration.steps.len(), schema.steps.len());
    for (declared, step) in declaration.steps.iter().zip(&schema.steps) {
        assert_eq!(declared.id, step.id);
        assert_eq!(declared.item, step.item);
        assert_eq!(declared.from, step.from);
        assert_eq!(declared.to, step.to);
        assert_eq!(declared.source.as_deref(), step.source);
        assert_eq!(declared.transform.operation, format!("sample.{}", step.id));
        assert_eq!(declared.transform.version, 1);
    }
    assert_eq!(
        declaration
            .steps
            .iter()
            .map(|step| step.transform.kind)
            .collect::<Vec<_>>(),
        vec![
            TransformKind::Documents,
            TransformKind::Stream,
            TransformKind::Blob,
        ]
    );
    Registry::compose_with_declarations(Vec::<Schema>::new(), [declaration])
        .expect("explicit descriptors compose as an external owner declaration");
}

#[test]
fn declaration_requires_a_mapping_for_every_step() {
    let schema = schema_with_all_transform_families();
    let error = declaration_from_schema(&schema, |step| {
        (step.id != "events_v2").then(|| (step.id.to_owned(), 1))
    })
    .expect_err("an unmapped local closure cannot be declared remotely");

    assert_eq!(error.kind(), crate::ErrorKind::InvalidInput);
    assert!(error.to_string().contains("events_v2"));
}

#[test]
fn exporting_metadata_preserves_the_original_local_transform() {
    let called = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&called);
    let schema = Schema::new(Owner::new("local").expect("valid owner"))
        .collection("local.records", 2, ScopeClass::Tenant)
        .step(Step::new(
            "records_v2",
            "local.records",
            1,
            2,
            move |records| {
                observed.fetch_add(1, Ordering::AcqRel);
                Ok(records
                    .into_iter()
                    .map(|record| {
                        TargetRecord::new(
                            "local.records",
                            record.id,
                            json!({"migrated": record.body}),
                        )
                    })
                    .collect())
            },
        ));
    let declaration = declaration_from_schema(&schema, |_| Some(("records_v2".to_owned(), 1)))
        .expect("the test schema declares its remote operation");
    assert_eq!(declaration.steps.len(), 1);

    let StepBody::Documents(transform) = &schema.steps[0].run else {
        panic!("the source schema retains its local document callback");
    };
    let output = transform(vec![SourceRecord {
        id: "row-1".to_owned(),
        body: json!({"legacy": true}),
        meta: Meta::default(),
    }])
    .expect("the original local callback remains executable");

    assert_eq!(called.load(Ordering::Acquire), 1);
    assert_eq!(output.len(), 1);
    assert_eq!(output[0].collection, "local.records");
    assert_eq!(output[0].id, "row-1");
    assert_eq!(output[0].body, json!({"migrated": {"legacy": true}}));
}
