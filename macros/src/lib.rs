extern crate proc_macro;
use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, ItemFn};

#[proc_macro_attribute]
pub fn unit_test(_: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemFn);
    let name = &input.sig.ident;
    let block = &input.block;

    let expanded = quote! {
        #[tokio::test]
        async fn #name() {
            dotenv::dotenv().ok();
            #block
        }
    };

    TokenStream::from(expanded)
}
