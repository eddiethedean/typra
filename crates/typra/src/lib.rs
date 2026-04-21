//! Application-facing Typra API.
//!
//! This crate re-exports the engine ([`typra_core`]) and, with the default **`derive`**
//! feature, the [`DbModel`](typra_derive::DbModel) procedural macro.
//!
//! ```toml
//! [dependencies]
//! typra = "0.2"
//! ```
//!
//! For lower-level control, depend on [`typra-core`] and [`typra-derive`] directly.

pub use typra_core::*;

#[cfg(feature = "derive")]
pub use typra_derive::DbModel;

/// Common imports for application code.
pub mod prelude {
    pub use typra_core::prelude::*;

    #[cfg(feature = "derive")]
    pub use crate::DbModel;
}
