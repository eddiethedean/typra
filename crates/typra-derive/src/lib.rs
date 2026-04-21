use proc_macro::TokenStream;

#[proc_macro_derive(DbModel, attributes(db))]
pub fn derive_db_model(_input: TokenStream) -> TokenStream {
    TokenStream::new()
}
