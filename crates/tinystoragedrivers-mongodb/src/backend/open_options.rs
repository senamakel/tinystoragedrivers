//! Requirements checked while a MongoDB backend opens.

use tinystoragedrivers_core::{Capability, Result, StorageError};

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

    pub(super) fn validate(self, transactions_supported: bool) -> Result<()> {
        if self.require_fencing && !transactions_supported {
            return Err(StorageError::unsupported(
                Capability::Fencing,
                "this MongoDB deployment does not provide fencing",
            ));
        }
        if self.require_transactions && !transactions_supported {
            return Err(StorageError::unsupported(
                Capability::Transactions,
                "this MongoDB deployment does not provide transactions",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "open_options_tests.rs"]
mod tests;
