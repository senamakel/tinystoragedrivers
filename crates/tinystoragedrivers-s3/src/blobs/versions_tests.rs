//! S3 version-list response parsing and continuation contracts.

use super::*;

#[test]
fn parses_versions_delete_markers_and_null_version_ids() {
    let page = parse_page(
        r#"<ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <IsTruncated>true</IsTruncated>
            <NextKeyMarker>scope/key</NextKeyMarker>
            <NextVersionIdMarker>v2</NextVersionIdMarker>
            <Version><Key>scope/key</Key><VersionId>null</VersionId></Version>
            <Version><Key>scope/key</Key><VersionId>v2</VersionId></Version>
            <DeleteMarker><Key>scope/key</Key><VersionId>delete-3</VersionId></DeleteMarker>
        </ListVersionsResult>"#,
    )
    .unwrap();

    assert_eq!(page.versions.len(), 2);
    assert_eq!(page.versions[0].version_id, "null");
    assert_eq!(page.delete_markers.len(), 1);
    assert_eq!(page.delete_markers[0].version_id, "delete-3");
    assert_eq!(
        next_markers(&page, None, None).unwrap(),
        ("scope/key".to_owned(), Some("v2".to_owned()))
    );
}

#[test]
fn truncated_version_list_can_advance_to_a_new_key_without_a_version_marker() {
    let page = parse_page(
        "<ListVersionsResult><IsTruncated>true</IsTruncated><NextKeyMarker>next</NextKeyMarker></ListVersionsResult>",
    )
    .unwrap();

    assert_eq!(
        next_markers(&page, Some("previous"), Some("old-version")).unwrap(),
        ("next".to_owned(), None)
    );
}

#[test]
fn truncated_version_list_requires_advancing_complete_markers() {
    let missing_marker =
        parse_page("<ListVersionsResult><IsTruncated>true</IsTruncated></ListVersionsResult>")
            .unwrap();
    assert!(next_markers(&missing_marker, None, None).is_err());

    let repeating_marker = parse_page(
        "<ListVersionsResult><IsTruncated>true</IsTruncated><NextKeyMarker>k</NextKeyMarker><NextVersionIdMarker>v</NextVersionIdMarker></ListVersionsResult>",
    )
    .unwrap();
    assert!(next_markers(&repeating_marker, Some("k"), Some("v")).is_err());

    let same_key_advanced_version = parse_page(
        "<ListVersionsResult><IsTruncated>true</IsTruncated><NextKeyMarker>k</NextKeyMarker><NextVersionIdMarker>v2</NextVersionIdMarker></ListVersionsResult>",
    )
    .unwrap();
    assert_eq!(
        next_markers(&same_key_advanced_version, Some("k"), Some("v1")).unwrap(),
        ("k".to_owned(), Some("v2".to_owned()))
    );

    let empty_marker = parse_page(
        "<ListVersionsResult><IsTruncated>true</IsTruncated><NextKeyMarker></NextKeyMarker></ListVersionsResult>",
    )
    .unwrap();
    assert!(next_markers(&empty_marker, None, None).is_err());
}

#[test]
fn malformed_version_list_is_a_backend_error_without_returning_body_text() {
    let error = parse_page(
        "<ListVersionsResult><IsTruncated>not-a-boolean</IsTruncated></ListVersionsResult>",
    )
    .unwrap_err();
    assert_eq!(error.kind(), tinystoragedrivers_core::ErrorKind::Backend);
    assert_eq!(
        error.to_string(),
        "backend: invalid S3 version-list response"
    );
}

#[test]
fn rejects_a_non_version_list_root_even_when_its_fields_look_valid() {
    let error = parse_page(
        "<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>scope/key</Key></Contents></ListBucketResult>",
    )
    .unwrap_err();
    assert_eq!(error.kind(), tinystoragedrivers_core::ErrorKind::Backend);
}

#[test]
fn rejects_a_version_without_an_id() {
    let error = parse_page(
        "<ListVersionsResult><IsTruncated>false</IsTruncated><Version><Key>scope/key</Key></Version></ListVersionsResult>",
    )
    .unwrap_err();
    assert_eq!(error.kind(), tinystoragedrivers_core::ErrorKind::Backend);
}

#[test]
fn rejects_a_version_with_an_empty_id() {
    let error = parse_page(
        "<ListVersionsResult><IsTruncated>false</IsTruncated><Version><Key>scope/key</Key><VersionId></VersionId></Version></ListVersionsResult>",
    )
    .unwrap_err();
    assert_eq!(error.kind(), tinystoragedrivers_core::ErrorKind::Backend);
}

#[test]
fn rejects_a_version_with_an_empty_key() {
    let error = parse_page(
        "<ListVersionsResult><IsTruncated>false</IsTruncated><Version><Key></Key><VersionId>v1</VersionId></Version></ListVersionsResult>",
    )
    .unwrap_err();
    assert_eq!(error.kind(), tinystoragedrivers_core::ErrorKind::Backend);
}
