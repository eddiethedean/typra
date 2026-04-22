//! Procedural macro crate: derives [`typra_core::schema::DbModel`] for Rust types.

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput};

/// Derive the [`typra_core::schema::DbModel`] marker trait (no fields or methods yet).
///
/// Intended for use with the `typra` facade crate’s **`derive`** feature.
#[proc_macro_derive(DbModel, attributes(db))]
pub fn derive_db_model(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    let expanded = quote! {
        impl #impl_generics ::typra_core::DbModel for #name #ty_generics #where_clause {}
    };

    TokenStream::from(expanded)
}
