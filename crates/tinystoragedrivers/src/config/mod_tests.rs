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
            database: "app".into(),
            require_fencing: false,
            require_transactions: false,
        }
    );
}

#[test]
fn mongo_url_options_are_removed_without_disturbing_driver_options() {
    let config = StorageConfig::parse(
        "mongodb://user:secret@host/db?retryWrites=true&require_fencing=true&appName=tsd&require_transactions=true",
    )
    .unwrap();
    let StorageConfig::MongoDb {
        uri,
        require_fencing,
        require_transactions,
        ..
    } = &config
    else {
        panic!("expected a MongoDB configuration")
    };
    assert_eq!(uri, "mongodb://user:secret@host/db?retryWrites=true&appName=tsd");
    assert!(*require_fencing);
    assert!(*require_transactions);
    assert_eq!(
        config.to_string(),
        "mongodb://user:***@host/db?retryWrites=true&appName=tsd&require_fencing=true&require_transactions=true"
    );
}

#[test]
fn mongo_url_requirement_options_reject_invalid_and_duplicate_values() {
    for url in [
        "mongodb://host/db?require_fencing=yes",
        "mongodb://host/db?require_transactions=1",
        "mongodb://host/db?require_fencing=true&require_fencing=false",
        "mongodb://host/db?require_transactions=true&require_transactions=true",
    ] {
        let error = StorageConfig::parse(url).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput, "{url}");
    }
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

#[cfg(feature = "mongodb")]
#[tokio::test]
async fn live_mongodb_url_enforces_required_capabilities() {
    let Some(uri) = std::env::var("TSD_MONGO_URL")
        .ok()
        .filter(|uri| !uri.is_empty())
    else {
        eprintln!("live_mongodb_url_enforces_required_capabilities: TSD_MONGO_URL is not set; skipping");
        return;
    };
    let separator = if uri.contains('?') { '&' } else { '?' };
    let url = format!(
        "{uri}{separator}require_fencing=true&require_transactions=true&retryWrites=true"
    );
    let backend = open(&StorageConfig::parse(&url).unwrap()).await.unwrap();
    assert!(backend.capabilities().contains(tinystoragedrivers_core::Capability::Fencing));
    assert!(backend
        .capabilities()
        .contains(tinystoragedrivers_core::Capability::Transactions));
}

#[cfg(feature = "mongodb")]
#[tokio::test]
async fn live_mongodb_url_refuses_missing_standalone_requirements() {
    let Some(uri) = std::env::var("TSD_MONGO_STANDALONE_URL")
        .ok()
        .filter(|uri| !uri.is_empty())
    else {
        eprintln!("live_mongodb_url_refuses_missing_standalone_requirements: TSD_MONGO_STANDALONE_URL is not set; skipping");
        return;
    };
    let separator = if uri.contains('?') { '&' } else { '?' };
    for (requirement, capability) in [
        ("require_fencing=true", tinystoragedrivers_core::Capability::Fencing),
        (
            "require_transactions=true",
            tinystoragedrivers_core::Capability::Transactions,
        ),
    ] {
        let url = format!("{uri}{separator}{requirement}");
        let config = StorageConfig::parse(&url).unwrap();
        let error = open(&config).await.unwrap_err();
        assert_eq!(
            error.kind(),
            tinystoragedrivers_core::ErrorKind::Unsupported(capability)
        );
    }
}
