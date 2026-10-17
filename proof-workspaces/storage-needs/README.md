# Storage-needs dependency proof

This independent workspace models a module contract crate that links only the
canonical storage bus declarations. It disables default features, constructs a
`StorageNeeds` with index, TTL and search metadata, and exercises every
resource-prefix field. It also wraps the complete legacy declaration with an
exact secret-name need, while preserving the original struct-literal API. The
companion CI script runs it and inspects its
resolved normal/build dependencies to ensure the compatibility client does not
pull TinyBus or an implementation dependency into data-only consumers. Its
package MSRV is Rust 1.85, and CI runs the locked proof with that toolchain.
