//! Application-facing ModelVault API: re-exports [`modelvault_core`] and optionally the [`DbModel`](modelvault_derive::DbModel) derive.
//!
//! Add to `Cargo.toml`:
//!
//! ```toml
//! [dependencies]
//! modelvault = "0.8"
//! ```
//!
//! Use [`prelude`] for common imports. For full control over dependencies, depend on the
//! `modelvault-core` and `modelvault-derive` crates directly.

pub use modelvault_core::*;

#[cfg(feature = "derive")]
pub use modelvault_derive::DbModel;

#[cfg(feature = "async")]
pub mod async_api;
#[cfg(feature = "async")]
pub use async_api::AsyncDatabase;

/// Re-exports [`modelvault_core::prelude`] plus [`DbModel`](modelvault_derive::DbModel) when **`derive`** is enabled.
pub mod prelude {
    pub use modelvault_core::prelude::*;

    #[cfg(feature = "derive")]
    pub use crate::DbModel;

    #[cfg(feature = "async")]
    pub use crate::AsyncDatabase;
}
