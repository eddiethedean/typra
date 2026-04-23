//! Procedural macro crate: derives [`typra_core::schema::DbModel`] for Rust types.

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Fields, GenericArgument, PathArguments, Type};

/// Derive the [`typra_core::schema::DbModel`] trait for structs.
///
/// Intended for use with the `typra` facade crate’s **`derive`** feature.
#[proc_macro_derive(DbModel, attributes(db))]
pub fn derive_db_model(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    let collection_name = parse_collection_name(&input).unwrap_or_else(|| name.to_string());

    let Data::Struct(data_struct) = &input.data else {
        return syn::Error::new_spanned(name, "DbModel can only be derived for structs")
            .to_compile_error()
            .into();
    };

    let Fields::Named(fields) = &data_struct.fields else {
        return syn::Error::new_spanned(name, "DbModel requires named fields")
            .to_compile_error()
            .into();
    };

    let mut primary_field: Option<String> = None;
    let mut field_defs = Vec::new();
    let mut index_defs = Vec::new();

    for f in &fields.named {
        let Some(ident) = &f.ident else { continue };
        let field_name = ident.to_string();
        let (is_primary, is_unique, is_index) = parse_field_flags(f);

        if is_primary {
            if primary_field.is_some() {
                return syn::Error::new_spanned(
                    ident,
                    "exactly one field must be marked #[db(primary)]",
                )
                .to_compile_error()
                .into();
            }
            primary_field = Some(field_name.clone());
        }

        let ty_expr = match rust_type_to_typra_type(&f.ty) {
            Ok(t) => t,
            Err(e) => return e.to_compile_error().into(),
        };

        field_defs.push(quote! {
            ::typra_core::FieldDef {
                path: ::typra_core::schema::FieldPath(vec![::std::borrow::Cow::Borrowed(#field_name)]),
                ty: #ty_expr,
                constraints: ::std::vec::Vec::new(),
            }
        });

        if is_unique {
            let idx_name = format!("{field_name}_unique");
            index_defs.push(quote! {
                ::typra_core::schema::IndexDef {
                    name: #idx_name.to_string(),
                    path: ::typra_core::schema::FieldPath(vec![::std::borrow::Cow::Borrowed(#field_name)]),
                    kind: ::typra_core::schema::IndexKind::Unique,
                }
            });
        } else if is_index {
            let idx_name = format!("{field_name}_idx");
            index_defs.push(quote! {
                ::typra_core::schema::IndexDef {
                    name: #idx_name.to_string(),
                    path: ::typra_core::schema::FieldPath(vec![::std::borrow::Cow::Borrowed(#field_name)]),
                    kind: ::typra_core::schema::IndexKind::NonUnique,
                }
            });
        }
    }

    let Some(primary_field) = primary_field else {
        return syn::Error::new_spanned(
            name,
            "missing primary key: mark one field with #[db(primary)]",
        )
        .to_compile_error()
        .into();
    };

    let expanded = quote! {
        impl #impl_generics ::typra_core::DbModel for #name #ty_generics #where_clause {
            fn collection_name() -> &'static str {
                #collection_name
            }

            fn fields() -> ::std::vec::Vec<::typra_core::FieldDef> {
                ::std::vec![#(#field_defs),*]
            }

            fn primary_field() -> &'static str {
                #primary_field
            }

            fn indexes() -> ::std::vec::Vec<::typra_core::schema::IndexDef> {
                ::std::vec![#(#index_defs),*]
            }
        }
    };

    TokenStream::from(expanded)
}

fn parse_collection_name(input: &DeriveInput) -> Option<String> {
    for attr in &input.attrs {
        if !attr.path().is_ident("db") {
            continue;
        }
        let mut out: Option<String> = None;
        let _ = attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("collection") {
                let value = meta.value()?;
                let lit: syn::LitStr = value.parse()?;
                out = Some(lit.value());
            }
            Ok(())
        });
        if out.is_some() {
            return out;
        }
    }
    None
}

fn parse_field_flags(field: &syn::Field) -> (bool, bool, bool) {
    let mut primary = false;
    let mut unique = false;
    let mut index = false;
    for attr in &field.attrs {
        if !attr.path().is_ident("db") {
            continue;
        }
        let _ = attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("primary") {
                primary = true;
            } else if meta.path.is_ident("unique") {
                unique = true;
            } else if meta.path.is_ident("index") {
                index = true;
            }
            Ok(())
        });
    }
    (primary, unique, index)
}

fn rust_type_to_typra_type(ty: &Type) -> Result<proc_macro2::TokenStream, syn::Error> {
    match ty {
        Type::Path(p) => {
            let seg = p
                .path
                .segments
                .last()
                .ok_or_else(|| syn::Error::new_spanned(ty, "unsupported type (empty path)"))?;
            let ident = seg.ident.to_string();

            match (ident.as_str(), &seg.arguments) {
                ("bool", _) => Ok(quote! { ::typra_core::schema::Type::Bool }),
                ("i8" | "i16" | "i32" | "i64", _) => Ok(quote! { ::typra_core::schema::Type::Int64 }),
                ("u8" | "u16" | "u32" | "u64", _) => Ok(quote! { ::typra_core::schema::Type::Uint64 }),
                ("f32" | "f64", _) => Ok(quote! { ::typra_core::schema::Type::Float64 }),
                ("String", _) => Ok(quote! { ::typra_core::schema::Type::String }),
                ("Vec", PathArguments::AngleBracketed(args)) => {
                    let inner = args.args.first().ok_or_else(|| {
                        syn::Error::new_spanned(ty, "Vec<T> requires one type argument")
                    })?;
                    let GenericArgument::Type(inner_ty) = inner else {
                        return Err(syn::Error::new_spanned(inner, "unsupported Vec<T> argument"));
                    };
                    if is_u8(inner_ty) {
                        Ok(quote! { ::typra_core::schema::Type::Bytes })
                    } else {
                        let inner_expr = rust_type_to_typra_type(inner_ty)?;
                        Ok(quote! { ::typra_core::schema::Type::List(::std::boxed::Box::new(#inner_expr)) })
                    }
                }
                ("Option", PathArguments::AngleBracketed(args)) => {
                    let inner = args.args.first().ok_or_else(|| {
                        syn::Error::new_spanned(ty, "Option<T> requires one type argument")
                    })?;
                    let GenericArgument::Type(inner_ty) = inner else {
                        return Err(syn::Error::new_spanned(inner, "unsupported Option<T> argument"));
                    };
                    let inner_expr = rust_type_to_typra_type(inner_ty)?;
                    Ok(quote! { ::typra_core::schema::Type::Optional(::std::boxed::Box::new(#inner_expr)) })
                }
                _ => Err(syn::Error::new_spanned(
                    ty,
                    "unsupported Rust field type for DbModel (use primitives, String, Option<T>, Vec<T>)",
                )),
            }
        }
        _ => Err(syn::Error::new_spanned(
            ty,
            "unsupported Rust type for DbModel",
        )),
    }
}

fn is_u8(ty: &Type) -> bool {
    let Type::Path(p) = ty else { return false };
    p.path
        .segments
        .last()
        .map(|s| s.ident == "u8")
        .unwrap_or(false)
}
