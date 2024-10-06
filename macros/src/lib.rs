extern crate proc_macro;
use proc_macro::TokenStream;
use quote::quote;
use syn::punctuated::Punctuated;
use syn::token::Comma;
use syn::{parse_macro_input, FnArg, ItemFn};

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

#[proc_macro_attribute]
pub fn endpoint(args: TokenStream, item: TokenStream) -> TokenStream {
    /// This macro attribute is used to mark a function as an endpoint.
    /// It will wrap the function body with a transaction support and/or private auth.
    let input = parse_macro_input!(item as ItemFn);

    let mut transactional: bool = false;
    let mut private: bool = false;
    let endpoint_parser = syn::meta::parser(|meta| {
        if meta.path.is_ident("private") {
            private = true;
            Ok(())
        } else if meta.path.is_ident("transactional") {
            transactional = true;
            Ok(())
        } else {
            Err(meta.error("unsupported endpoint property"))
        }
    });

    parse_macro_input!(args with endpoint_parser);

    if !private && !transactional {
        panic!("At least one of the following attributes must be present: private, transactional");
    }

    let fn_visiblity = &input.vis;
    let fn_name = &input.sig.ident;
    let fn_body = &input.block;
    let fn_output = &input.sig.output;
    let asyncness = &input.sig.asyncness;
    let fn_args = input.sig.inputs;

    let mut new_fn_args: Punctuated<FnArg, Comma> = Punctuated::new();
    if private {
        new_fn_args.push(syn::parse_quote!(claims: axum_webtools::security::jwt::Claims));
    }
    if transactional {
        new_fn_args.push(syn::parse_quote!(State(pool): State<sqlx::PgPool>));
    }
    for arg in fn_args.iter().cloned() {
        new_fn_args.push(arg);
    }

    let expanded = quote! {
        #fn_visiblity #asyncness fn #fn_name(#new_fn_args) #fn_output {
            axum_webtools::db::sqlx::with_tx(&pool, move |tx| async move {
                #fn_body
            }.scope_boxed()).await
        }
    };

    TokenStream::from(expanded)
}
