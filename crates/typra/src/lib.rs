//! Application-facing Typra API: re-exports [`typra_core`] and optionally the [`DbModel`](typra_derive::DbModel) derive.
//!
//! Add to `Cargo.toml`:
//!
//! ```toml
//! [dependencies]
//! typra = "0.8"
//! ```
//!
//! Use [`prelude`] for common imports. For full control over dependencies, depend on the
//! `typra-core` and `typra-derive` crates directly.

pub use typra_core::*;

#[cfg(feature = "derive")]
pub use typra_derive::DbModel;

#[cfg(feature = "async")]
pub mod async_api;
#[cfg(feature = "async")]
pub use async_api::AsyncDatabase;

/// Re-exports [`typra_core::prelude`] plus [`DbModel`](typra_derive::DbModel) when **`derive`** is enabled.
pub mod prelude {
    pub use typra_core::prelude::*;

    #[cfg(feature = "derive")]
    pub use crate::DbModel;

    #[cfg(feature = "async")]
    pub use crate::AsyncDatabase;
}
