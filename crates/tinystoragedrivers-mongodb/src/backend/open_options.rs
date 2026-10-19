//! Requirements checked while a MongoDB backend opens.

/// Optional MongoDB capabilities a caller requires at open time.
///
/// By default, MongoDB opens on any reachable deployment and reports the
/// capabilities it supports. Use the builder methods when the application
/// depends on a capability that standalone MongoDB does not provide.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MongoOpenOptions {
    pub(super) require_fencing: bool,
    pub(super) require_transactions: bool,
}

impl MongoOpenOptions {
    /// Start with no required capabilities.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            require_fencing: false,
            require_transactions: false,
        }
    }

    /// Require `Capability::Fencing` from the connected deployment.
    #[must_use]
    pub const fn require_fencing(mut self) -> Self {
        self.require_fencing = true;
        self
    }

    /// Require `Capability::Transactions` from the connected deployment.
    #[must_use]
    pub const fn require_transactions(mut self) -> Self {
        self.require_transactions = true;
        self
    }
}

#[cfg(test)]
#[path = "open_options_tests.rs"]
mod tests;
