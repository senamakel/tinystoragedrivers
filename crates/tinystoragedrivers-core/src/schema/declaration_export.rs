//! Exporting local closure-backed schema metadata as a module declaration.

use tinystoragedrivers_schema::{SchemaDeclaration, StepDeclaration, TransformDescriptor};

use crate::{Result, StorageError};

use super::{Schema, Step, StepBody, TransformKind};

/// Build a transport declaration from one local schema without exporting its
/// executable closures.
///
/// `operation` must provide an explicit operation selector and contract
/// version for every step that the owner intends to expose remotely. The
/// declaration's transform family is derived from the actual step body.
///
/// # Errors
/// Returns [`StorageError::invalid_input`] when a step has no external
/// operation mapping.
pub fn declaration_from_schema(
    schema: &Schema,
    mut operation: impl FnMut(&Step) -> Option<(String, u32)>,
) -> Result<SchemaDeclaration> {
    let steps = schema
        .steps
        .iter()
        .map(|step| {
            let Some((operation, version)) = operation(step) else {
                return Err(StorageError::invalid_input(format!(
                    "schema step `{}` has no external transform mapping",
                    step.id
                )));
            };
            let kind = match step.run {
                StepBody::Documents(_) => TransformKind::Documents,
                StepBody::Streams(_) => TransformKind::Stream,
                StepBody::Blobs(_) => TransformKind::Blob,
            };
            Ok(StepDeclaration {
                id: step.id.to_owned(),
                item: step.item.to_owned(),
                from: step.from,
                to: step.to,
                source: step.source.map(str::to_owned),
                transform: TransformDescriptor {
                    operation,
                    version,
                    kind,
                },
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(SchemaDeclaration {
        owner: schema.owner.clone(),
        items: schema.items.clone(),
        steps,
    })
}
