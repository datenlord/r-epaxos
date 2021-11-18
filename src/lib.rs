mod types;
mod error;
mod message;

use proc_macro2::{TokenStream};
use syn::{Data, DeriveInput, Error, Fields, parse_macro_input, spanned::Spanned};

#[proc_macro_derive(FromU32)]
pub fn from_u32(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = input.ident;

    proc_macro::TokenStream::from(match input.data {
        Data::Struct(ref data) => {
            match data.fields {
                Fields::Named(ref fields) => {
                    if fields.named.len() != 1 {
                        Error::new(input.span(), "Expected only one field").to_compile_error();
                    }

                    fields.named.first().map(|f| {
                        f.ty
                    } )
                },
                Fields::Unnamed(_) => {},
                _ => {}
            }
        }
        _ => {}
    })
}
