use quote::quote;
use syn::{parse_macro_input, spanned::Spanned, Data, DeriveInput, Error, Fields};

#[proc_macro_derive(FromInner)]
pub fn from_inner(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    proc_macro::TokenStream::from(match input.data {
        Data::Struct(ref data) => match data.fields {
            Fields::Named(ref fields) => {
                if fields.named.len() != 1 {
                    Error::new(input.span(), "Expected only one field").to_compile_error()
                } else {
                    fields
                        .named
                        .first()
                        .map(|f| {
                            let t = &f.ty;
                            f.ident.as_ref().map(|ident| {
                                quote! {
                                    impl From<#t> for #name {
                                        fn from(value: #t) -> Self {
                                            Self {
                                                #ident: value,
                                            }
                                        }
                                    }

                                    impl std::ops::Deref for #name {
                                        type Target = #t;
                                        fn deref(&self) -> &Self::Target {
                                            &self.#ident
                                        }
                                    }
                                }
                            })
                        })
                        .unwrap()
                        .unwrap()
                }
            }
            Fields::Unnamed(ref fields) => {
                if fields.unnamed.len() != 1 {
                    Error::new(input.span(), "Expected only one field").to_compile_error()
                } else {
                    let t = &fields.unnamed.first().unwrap().ty;
                    quote! {
                        impl From<#t> for #name {
                            fn from(value: #t) -> Self {
                                Self(value)
                            }
                        }

                        impl std::ops::Deref for #name {
                            type Target = #t;
                            fn deref(&self) -> &Self::Target {
                                &self.0
                            }
                        }
                    }
                }
            }
            _ => {
                Error::new(input.span(), "Only support named and unnamed struct").to_compile_error()
            }
        },
        _ => Error::new(input.span(), "Only support named and unnamed struct").to_compile_error(),
    })
}
