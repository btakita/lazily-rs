use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{
    FnArg, ItemFn, Pat, ReturnType, Token, Type, parse_macro_input, punctuated::Punctuated,
    spanned::Spanned,
};

/// Declare a computed (derived) cell factory. Canonical construction sugar of
/// the Cell kernel (`#lzcellkernel`) — the guarded derived cell.
#[proc_macro_attribute]
pub fn computed(args: TokenStream, input: TokenStream) -> TokenStream {
    expand_factory("slot", args, input)
}

/// Declare a source (writable) cell factory. Canonical construction sugar of
/// the Cell kernel (`#lzcellkernel`) — the source cell written from outside.
#[proc_macro_attribute]
pub fn source(args: TokenStream, input: TokenStream) -> TokenStream {
    expand_factory("cell", args, input)
}

/// Deprecated v1 spelling of [`macro@computed`]. Kept as a construction-sugar
/// alias; prefer `#[lazily::computed]`.
#[proc_macro_attribute]
pub fn slot(args: TokenStream, input: TokenStream) -> TokenStream {
    expand_factory("slot", args, input)
}

/// Deprecated v1 spelling of [`macro@source`]. Kept as a construction-sugar
/// alias; prefer `#[lazily::source]`.
#[proc_macro_attribute]
pub fn cell(args: TokenStream, input: TokenStream) -> TokenStream {
    expand_factory("cell", args, input)
}

fn expand_factory(kind: &str, args: TokenStream, input: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args with Punctuated::<syn::Meta, Token![,]>::parse_terminated);
    if !args.is_empty() {
        return syn::Error::new(
            args.span(),
            "lazily factory decorators (#[lazily::source] / #[lazily::computed]) do not take arguments",
        )
        .to_compile_error()
        .into();
    }

    let item = parse_macro_input!(input as ItemFn);
    match expand_factory_item(kind, item) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

fn expand_factory_item(kind: &str, item: ItemFn) -> syn::Result<proc_macro2::TokenStream> {
    let attrs = item.attrs;
    let vis = item.vis;
    let sig = item.sig;
    let body = item.block;
    let name = sig.ident;

    if sig.asyncness.is_some() {
        return Err(syn::Error::new(
            sig.asyncness.span(),
            "lazily factory decorators do not support async functions",
        ));
    }
    if sig.constness.is_some() {
        return Err(syn::Error::new(
            sig.constness.span(),
            "lazily factory decorators do not support const functions",
        ));
    }
    if sig.unsafety.is_some() {
        return Err(syn::Error::new(
            sig.unsafety.span(),
            "lazily factory decorators do not support unsafe functions",
        ));
    }
    if sig.abi.is_some() {
        return Err(syn::Error::new(
            sig.abi.span(),
            "lazily factory decorators do not support extern functions",
        ));
    }
    if !sig.generics.params.is_empty() || sig.generics.where_clause.is_some() {
        return Err(syn::Error::new(
            sig.generics.span(),
            "lazily factory decorators currently support non-generic functions",
        ));
    }

    if sig.inputs.len() != 1 {
        return Err(syn::Error::new(
            sig.inputs.span(),
            "lazily factory decorators require exactly one context argument",
        ));
    }

    let (ctx_ident, ctx_family_ty) = match sig.inputs.first().expect("checked len") {
        FnArg::Typed(arg) => {
            let ident = match arg.pat.as_ref() {
                Pat::Ident(ident) => ident.ident.clone(),
                other => {
                    return Err(syn::Error::new(
                        other.span(),
                        "lazily factory context argument must be a plain identifier",
                    ));
                }
            };
            let Type::Reference(reference) = arg.ty.as_ref() else {
                return Err(syn::Error::new(
                    arg.ty.span(),
                    "lazily factory context argument must be a shared reference",
                ));
            };
            if reference.mutability.is_some() {
                return Err(syn::Error::new(
                    reference.mutability.span(),
                    "lazily factory context argument must be a shared reference",
                ));
            }
            (ident, reference.elem.clone())
        }
        FnArg::Receiver(receiver) => {
            return Err(syn::Error::new(
                receiver.span(),
                "lazily factory decorators do not support methods",
            ));
        }
    };

    let value_ty = match sig.output {
        ReturnType::Type(_, ty) => ty,
        ReturnType::Default => {
            return Err(syn::Error::new(
                name.span(),
                "lazily factory decorators require an explicit return type",
            ));
        }
    };

    let key_ident = format_ident!("__lazily_{kind}_factory_for_{name}");
    let handle_method_ident = format_ident!("__lazily_{kind}_handle");
    let handle_fn_ident = format_ident!("__lazily_{kind}_factory_for_{name}_handle");
    let handle_fn_ptr_ident = format_ident!("__LAZILY_{kind}_FACTORY_FOR_{name}_HANDLE");
    let schema_ty = quote! { <#ctx_family_ty as ::lazily::TypedContextFamily>::Schema };

    let memoized_call = match kind {
        "slot" => quote! {
            ::lazily::TypedFactoryContext::memoized_slot::<#key_ident, #value_ty, _>(
                #ctx_ident,
                move |#ctx_ident| #body,
            )
        },
        "cell" => quote! {
            ::lazily::TypedFactoryContext::memoized_cell::<#key_ident, #value_ty, _>(
                #ctx_ident,
                move |#ctx_ident| #body,
            )
        },
        // `kind` is a crate-internal literal — the four `#[proc_macro_attribute]`
        // entry points pass "slot" or "cell" and nothing parses it from user input.
        // The message still carries the value: a panic that names nothing is a
        // panic nobody can diagnose (#failclosedsweep).
        other => unreachable!("unknown lazily factory kind: {other}"),
    };

    let handle_ty = match kind {
        "slot" => quote! { ::lazily::TypedSlotHandle<#schema_ty, #value_ty> },
        "cell" => quote! { ::lazily::TypedCellHandle<#schema_ty, #value_ty> },
        // `kind` is a crate-internal literal — the four `#[proc_macro_attribute]`
        // entry points pass "slot" or "cell" and nothing parses it from user input.
        // The message still carries the value: a panic that names nothing is a
        // panic nobody can diagnose (#failclosedsweep).
        other => unreachable!("unknown lazily factory kind: {other}"),
    };

    let get_impl = match kind {
        "slot" => quote! {
            impl ::lazily::TypedGet<#schema_ty, ::lazily::TypedSlotFactorySource> for #name
            where
                #ctx_family_ty: ::lazily::TypedContextFamily,
            {
                type Output = #value_ty;

                fn get_typed(self, ctx: &::lazily::TypedContext<#schema_ty>) -> Self::Output {
                    self.#handle_method_ident(ctx).get(ctx)
                }
            }

            impl ::lazily::TypedGetRef<#schema_ty, ::lazily::TypedSlotFactorySource> for #name
            where
                #ctx_family_ty: ::lazily::TypedContextFamily,
            {
                type Output = #value_ty;

                fn get_typed_ref(
                    self,
                    ctx: &::lazily::TypedContextRef<'_, #schema_ty>,
                ) -> Self::Output {
                    self.#handle_method_ident(ctx).get_ref(ctx)
                }
            }
        },
        "cell" => quote! {
            impl ::lazily::TypedGet<#schema_ty, ::lazily::TypedCellFactorySource> for #name
            where
                #ctx_family_ty: ::lazily::TypedContextFamily,
            {
                type Output = #value_ty;

                fn get_typed(self, ctx: &::lazily::TypedContext<#schema_ty>) -> Self::Output {
                    self.#handle_method_ident(ctx).get(ctx)
                }
            }

            impl ::lazily::TypedGetRef<#schema_ty, ::lazily::TypedCellFactorySource> for #name
            where
                #ctx_family_ty: ::lazily::TypedContextFamily,
            {
                type Output = #value_ty;

                fn get_typed_ref(
                    self,
                    ctx: &::lazily::TypedContextRef<'_, #schema_ty>,
                ) -> Self::Output {
                    self.#handle_method_ident(ctx).get_ref(ctx)
                }
            }
        },
        // `kind` is a crate-internal literal — the four `#[proc_macro_attribute]`
        // entry points pass "slot" or "cell" and nothing parses it from user input.
        // The message still carries the value: a panic that names nothing is a
        // panic nobody can diagnose (#failclosedsweep).
        other => unreachable!("unknown lazily factory kind: {other}"),
    };

    let set_impl = match kind {
        "cell" => quote! {
            impl ::lazily::TypedSet<#schema_ty, #value_ty, ::lazily::TypedCellFactorySource>
                for #name
            where
                #ctx_family_ty: ::lazily::TypedContextFamily,
            {
                fn set_typed(self, ctx: &::lazily::TypedContext<#schema_ty>, value: #value_ty) {
                    self.#handle_method_ident(ctx).set(ctx, value);
                }
            }
        },
        "slot" => quote! {},
        // `kind` is a crate-internal literal — the four `#[proc_macro_attribute]`
        // entry points pass "slot" or "cell" and nothing parses it from user input.
        // The message still carries the value: a panic that names nothing is a
        // panic nobody can diagnose (#failclosedsweep).
        other => unreachable!("unknown lazily factory kind: {other}"),
    };

    Ok(quote! {
        #(#attrs)*
        #[allow(non_camel_case_types)]
        #[derive(Clone, Copy, Debug, Default)]
        #vis struct #name;

        impl #name {
            fn #handle_method_ident<C>(&self, #ctx_ident: &C) -> #handle_ty
            where
                #ctx_family_ty: ::lazily::TypedContextFamily,
                C: ::lazily::TypedFactoryContext<Schema = #schema_ty> + ?Sized,
            {
                #[allow(non_camel_case_types)]
                struct #key_ident;
                #memoized_call
            }
        }

        #get_impl
        #set_impl

        fn #handle_fn_ident(#ctx_ident: &#ctx_family_ty) -> #handle_ty
        where
            #ctx_family_ty: ::lazily::TypedContextFamily,
        {
            #name.#handle_method_ident(#ctx_ident)
        }

        #[allow(non_upper_case_globals)]
        static #handle_fn_ptr_ident: fn(&#ctx_family_ty) -> #handle_ty = #handle_fn_ident;

        impl ::std::ops::Deref for #name
        where
            #ctx_family_ty: ::lazily::TypedContextFamily,
        {
            type Target = fn(&#ctx_family_ty) -> #handle_ty;

            fn deref(&self) -> &Self::Target {
                &#handle_fn_ptr_ident
            }
        }
    })
}
