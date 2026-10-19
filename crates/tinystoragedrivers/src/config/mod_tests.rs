//! URL parsing, redaction and driver selection.

use super::*;
use tinystoragedrivers_core::ErrorKind;

#[test]
fn parses_every_form() {
    assert_eq!(
        StorageConfig::parse(" memory ").unwrap(),
        StorageConfig::Memory
    );
    assert_eq!(
        StorageConfig::parse("memory:").unwrap(),
        StorageConfig::Memory
    );
    assert_eq!(
        StorageConfig::parse("sqlite:/var/lib/app").unwrap(),
        StorageConfig::Sqlite {
            path: "/var/lib/app".into()
        }
    );
    assert_eq!(
        StorageConfig::parse("sqlite:///tmp/x.db").unwrap(),
        StorageConfig::Sqlite {
            path: "/tmp/x.db".into()
        }
    );
    assert_eq!(
        StorageConfig::parse("./state").unwrap(),
        StorageConfig::Sqlite {
            path: "./state".into()
        }
    );
    assert_eq!(
        StorageConfig::parse("../up").unwrap(),
        StorageConfig::Sqlite {
            path: "../up".into()
        }
    );
    assert_eq!(
        StorageConfig::parse("file:data").unwrap(),
        StorageConfig::File { dir: "data".into() }
    );
    assert_eq!(
        StorageConfig::parse("mongodb+srv://h.example/app?retryWrites=true").unwrap(),
        StorageConfig::MongoDb {
            uri: "mongodb+srv://h.example/app?retryWrites=true".into(),
            database: "app".into()
        }
    );
    assert_eq!(
        StorageConfig::parse("memory?blobs=memory%3a")
            .unwrap()
            .driver(),
        "composite"
    );
}

#[test]
fn parses_an_encoded_blob_backend_and_preserves_record_options() {
    let blobs = percent_encode(
        "mongodb://blob-user:blob-secret@blob-host/blobs?password=query-secret&readConcern=majority",
    );
    let url = format!(
        "mongodb://records-user:records-secret@records-host/app?appName=desktop&retryWrites=true&blobs={blobs}"
    );
    let config = StorageConfig::parse(&url).unwrap();

    assert_eq!(config.driver(), "composite");
    let StorageConfig::Composite { records, .. } = &config else {
        panic!("expected a composite config")
    };
    assert_eq!(
        records.to_string(),
        "mongodb://records-user:***@records-host/app?appName=desktop&retryWrites=true"
    );
    let shown = config.to_string();
    for preserved in [
        "appName",
        "desktop",
        "retryWrites",
        "true",
        "readConcern",
        "majority",
    ] {
        assert!(shown.contains(preserved), "{shown} omitted {preserved}");
    }
    for secret in ["records-secret", "blob-secret", "query-secret"] {
        assert!(!shown.contains(secret), "{shown} exposed {secret}");
        assert!(!format!("{config:?}").contains(secret));
    }
}

#[cfg(feature = "s3")]
#[test]
fn parses_an_encoded_s3_blob_backend_without_connecting() {
    let blobs = percent_encode(
        "s3://test-bucket/root?region=us-east-1&endpoint=http%3A%2F%2F127.0.0.1%3A1",
    );
    let config = StorageConfig::parse(&format!("memory?blobs={blobs}")).unwrap();
    assert_eq!(config.driver(), "composite");
}

#[test]
fn rejects_malformed_duplicate_and_recursive_blob_parameters() {
    let malformed = StorageConfig::parse("memory?blobs=%ZZ").unwrap_err();
    assert_eq!(malformed.kind(), ErrorKind::InvalidInput);

    for bad in ["memory?blobs=", "memory?blobs=%FF"] {
        let error = StorageConfig::parse(bad).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput, "{bad}");
    }

    let duplicate =
        StorageConfig::parse("memory?blobs=memory&blobs=file%3A%2Ftmp%2Fblobs").unwrap_err();
    assert_eq!(duplicate.kind(), ErrorKind::InvalidInput);

    let recursive = StorageConfig::parse("memory?blobs=memory%3Fblobs%3Dmemory").unwrap_err();
    assert_eq!(recursive.kind(), ErrorKind::InvalidInput);
}

#[test]
fn nested_blob_parse_errors_do_not_echo_credentials() {
    let blobs =
        percent_encode("mongodb://nested-user:nested-secret@blob-host?password=query-secret");
    let error = StorageConfig::parse(&format!("memory?blobs={blobs}")).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert!(!error.to_string().contains("nested-secret"), "{error}");
    assert!(!error.to_string().contains("query-secret"), "{error}");
    assert!(error.message().contains("names no database"), "{error}");
}

#[test]
fn nested_blob_errors_redact_credentials_even_for_unknown_schemes() {
    let nested = percent_encode("memory?password=sentinel-secret");
    let error = StorageConfig::parse(&format!("memory?blobs={nested}")).unwrap_err();

    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert!(!error.to_string().contains("sentinel-secret"), "{error}");
}

#[cfg(feature = "file")]
#[tokio::test]
async fn opens_composite_url_with_independent_record_and_blob_drivers() {
    let dir = tempfile::tempdir().unwrap();
    let blobs_url = format!("file:{}", dir.path().display());
    let url = format!("memory?blobs={}", percent_encode(&blobs_url));
    let config = StorageConfig::parse(&url).unwrap();

    let backend = open(&config).await.unwrap();
    assert_eq!(backend.driver(), "composite");
    assert_eq!(backend.document_driver(), "memory");
    let scoped = backend.for_scope(&crate::Scope::local()).unwrap();
    scoped
        .blobs()
        .put("persisted", vec![1, 2, 3], None)
        .await
        .unwrap();

    let reopened = open(&StorageConfig::parse(&url).unwrap()).await.unwrap();
    let reopened = reopened.for_scope(&crate::Scope::local()).unwrap();
    assert_eq!(
        reopened
            .blobs()
            .get("persisted")
            .await
            .unwrap()
            .unwrap()
            .bytes,
        vec![1, 2, 3]
    );
}

fn percent_encode(value: &str) -> String {
    let mut encoded = String::new();
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            encoded.push(char::from(byte));
        } else {
            use std::fmt::Write as _;
            write!(&mut encoded, "%{byte:02X}").unwrap();
        }
    }
    encoded
}

#[test]
fn rejects_unusable_urls() {
    for bad in [
        "",
        "   ",
        "sqlite:",
        "file:",
        "postgres://h/db",
        "relative/path",
        "mongodb://h",
        "mongodb://h/",
        "mongodb://h/?x=1",
    ] {
        let error = StorageConfig::parse(bad).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput, "{bad:?}");
    }
}

#[test]
fn never_prints_a_password() {
    let config = StorageConfig::parse("mongodb://app:s3cret@h:27017/db").unwrap();
    assert_eq!(config.to_string(), "mongodb://app:***@h:27017/db");
    assert!(!format!("{config:?}").contains("s3cret"));
    let error = StorageConfig::parse("mongodb://app:s3cret@h:27017").unwrap_err();
    assert!(!error.to_string().contains("s3cret"), "{error}");
    let error = StorageConfig::parse("redis://app:s3cret@h/0").unwrap_err();
    assert!(!error.to_string().contains("s3cret"), "{error}");
    assert_eq!(redact("mongodb://user@h/db"), "mongodb://user@h/db");
    assert_eq!(redact("mongodb://h/db"), "mongodb://h/db");
    assert_eq!(redact("no-scheme"), "no-scheme");
}

#[test]
fn names_its_driver_and_renders() {
    let cases = [
        ("memory", "memory", "memory"),
        ("sqlite:/d", "sqlite", "sqlite:/d"),
        ("file:/d", "file", "file:/d"),
        ("mongodb://h/db", "mongodb", "mongodb://h/db"),
    ];
    for (url, driver, shown) in cases {
        let config = StorageConfig::parse(url).unwrap();
        assert_eq!(config.driver(), driver);
        assert_eq!(config.to_string(), shown);
    }
}

#[tokio::test]
async fn opens_memory_and_names_missing_features() {
    let backend = open(&StorageConfig::Memory).await.unwrap();
    assert_eq!(backend.driver(), "memory");
    let mut missing = Vec::new();
    if cfg!(not(feature = "sqlite")) {
        missing.push("sqlite:/tmp/x");
    }
    if cfg!(not(feature = "mongodb")) {
        missing.push("mongodb://h/db");
    }
    if cfg!(not(feature = "file")) {
        missing.push("file:/tmp/x");
    }
    for url in missing {
        let config = StorageConfig::parse(url).unwrap();
        let error = open(&config).await.map(|_| ()).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert!(
            error
                .message()
                .contains(&format!("`{}` driver", config.driver())),
            "{error}"
        );
    }
}

#[cfg(feature = "file")]
#[tokio::test]
async fn opens_a_file_url_with_the_file_feature() {
    let dir = tempfile::tempdir().unwrap();
    let config = StorageConfig::parse(&format!("file:{}", dir.path().display())).unwrap();
    let backend = open(&config).await.unwrap();
    assert_eq!(backend.driver(), "file");

    let file = dir.path().join("not-a-dir");
    std::fs::write(&file, b"x").unwrap();
    let blocked = StorageConfig::File { dir: file };
    let error = open(&blocked).await.map(|_| ()).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Backend);
}

#[test]
fn redaction_survives_unencoded_credentials_and_secret_options() {
    let slash = StorageConfig::parse("mongodb://app:p/a@ss@host/db").unwrap();
    assert_eq!(slash.to_string(), "mongodb://app:***@host/db");
    let StorageConfig::MongoDb { database, .. } = &slash else {
        panic!("expected a MongoDB config")
    };
    assert_eq!(database, "db");

    let aws = StorageConfig::parse(
        "mongodb+srv://h/db?authMechanism=MONGODB-AWS&authMechanismProperties=AWS_SESSION_TOKEN:tok&retryWrites=true",
    )
    .unwrap();
    let shown = aws.to_string();
    assert!(!shown.contains("tok"), "{shown}");
    assert!(shown.contains("authMechanismProperties=***"), "{shown}");
    assert!(shown.contains("retryWrites=true"), "{shown}");

    assert_eq!(
        redact("mongodb://h/?tlsCertificateKeyFilePassword=pw&x"),
        "mongodb://h/?tlsCertificateKeyFilePassword=***&x"
    );
    assert_eq!(redact("mongodb://h?x=1"), "mongodb://h/?x=1");
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn opens_sqlite_directories_and_files() {
    let dir = tempfile::tempdir().unwrap();
    let url = format!("sqlite:{}", dir.path().display());
    let backend = open(&StorageConfig::parse(&url).unwrap()).await.unwrap();
    assert_eq!(backend.driver(), "sqlite");
    assert!(dir.path().join("storage.db").exists());
    let file = dir.path().join("single.db");
    let backend = open(&StorageConfig::Sqlite { path: file.clone() })
        .await
        .unwrap();
    assert_eq!(backend.driver(), "sqlite");
    assert!(file.exists());
}

#[test]
fn a_question_mark_in_a_password_stays_hidden() {
    let shown = redact("mongodb://app:p?assword@host/db");
    assert_eq!(shown, "mongodb://app:***@host/db");
    let error = StorageConfig::parse("mongodb://app:p?assword@host").unwrap_err();
    assert!(!error.to_string().contains("assword"), "{error}");
    let query_at = StorageConfig::parse("mongodb://h/db?appName=a@b").unwrap();
    let StorageConfig::MongoDb { database, .. } = &query_at else {
        panic!("expected a MongoDB config")
    };
    assert_eq!(database, "db", "an @ in a query value is not userinfo");
}

#[cfg(feature = "mongodb")]
#[tokio::test]
async fn opens_a_mongodb_url_with_the_driver() {
    // A port that is not a number fails the driver's own URI parse before
    // any I/O, so this proves the URL reaches the driver without a network.
    let config = StorageConfig::parse("mongodb://app:s3cret@h:notaport/db").unwrap();
    let error = open(&config).await.map(|_| ()).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert!(
        !error.message().contains("not available in this build"),
        "the driver, not the missing-feature arm, answered: {error}"
    );
    assert!(!error.to_string().contains("s3cret"), "{error}");
}
