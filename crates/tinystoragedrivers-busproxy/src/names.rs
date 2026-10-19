//! Where the storage objects live on the bus, and how errors are named.
use tinybus::{BusName, InterfaceName, MemberName, ObjectPath};
use tinystoragedrivers_bus::STORAGE_INTERFACE;
use tinystoragedrivers_client::Object;

/// The well-known name the host owns, and the interface its objects speak.
pub const HOST_NAME: &str = STORAGE_INTERFACE;
/// Where the five objects are served: `<root>/<Object>`.
pub const OBJECT_ROOT: &str = "/ai/tinyhumans/tinystorage";
/// Error name for a call the host refused before reaching a backend.
pub const ERROR_DENIED: &str = "ai.tinyhumans.tinystorage.Error.Denied";
/// Error name for a backend failure; the message is the
/// `snake_case` `StorageKind`.
pub const ERROR_STORAGE: &str = "ai.tinyhumans.tinystorage.Error.Storage";
/// Signal member that carries a payload-free `WatchChanged`.
pub const WATCH_MEMBER: &str = "WatchChanged";

/// A name this crate spells itself, which is valid by construction.
pub(crate) fn fixed<T>(parsed: tinybus::Result<T>) -> T {
    parsed.unwrap_or_else(|_| unreachable!("a name this crate fixes is valid"))
}

/// The object path serving `object`.
#[must_use]
pub fn object_path(object: Object) -> ObjectPath {
    fixed(ObjectPath::new(format!(
        "{OBJECT_ROOT}/{}",
        object.as_str()
    )))
}

/// The storage interface name.
pub(crate) fn interface() -> InterfaceName {
    fixed(InterfaceName::new(STORAGE_INTERFACE))
}

/// A fixed member name.
pub(crate) fn member(name: &str) -> MemberName {
    fixed(MemberName::new(name))
}

/// The host's well-known bus name.
pub(crate) fn host_name() -> BusName {
    fixed(BusName::new(HOST_NAME))
}

/// A wire value for a plain derived-`Serialize` message, which cannot fail.
pub(crate) fn wire_value<T: serde::Serialize>(message: &T) -> serde_json::Value {
    serde_json::to_value(message).unwrap_or(serde_json::Value::Null)
}

/// The path the host emits `recipient`'s watch signals on.
///
/// Watch ids are minted per caller binding (`w1`, `w2`, ...), so two callers'
/// signals cannot share an address: each caller subscribes to the path derived
/// from its own unique bus name, and only the host's unique name is accepted
/// as the sender. A unique name `:1.17` becomes `_1_17`, which is injective
/// for names of that shape.
///
/// # Errors
/// A recipient that is not a unique bus name.
pub fn watch_path(recipient: &BusName) -> tinybus::Result<ObjectPath> {
    watch_path_at(OBJECT_ROOT, recipient)
}

/// Watch path under one lease's exact object root.
pub(crate) fn watch_path_at(root: &str, recipient: &BusName) -> tinybus::Result<ObjectPath> {
    if !recipient.is_unique() {
        return Err(tinybus::Error::failed(
            "watch signals address a unique bus name",
        ));
    }
    let element: String = recipient
        .as_str()
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect();
    ObjectPath::new(format!("{root}/Watch/{element}"))
}

#[cfg(test)]
#[path = "names_tests.rs"]
mod tests;
