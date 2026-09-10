use super::{
    facts::{self, Fact, Item, Source, tokens},
    imports::Imports,
};
use proc_macro2::Span;
use syn::{
    spanned::Spanned,
    visit::{self, Visit},
};

mod attributes;
mod macro_attributes;

struct Scan {
    output: Source,
    owners: Vec<String>,
    test_only: bool,
    explicit_test_cfg: bool,
    imports: Imports,
}

pub(super) fn source(path: &str, source: &str) -> syn::Result<Source> {
    let file = syn::parse_file(source)?;
    let mut scan = Scan {
        output: Source {
            path: path.to_owned(),
            tokens: tokens(&file),
            test_only_file: facts::explicit_test_cfg(&file.attrs),
            ..Source::default()
        },
        owners: vec!["crate".to_owned()],
        test_only: path.starts_with("src/dst/")
            || path.contains("/tests/")
            || path.ends_with("_tests.rs"),
        explicit_test_cfg: false,
        imports: Imports::default().with_items(&file.items),
    };
    scan.visit_file(&file);
    Ok(scan.output)
}

impl Scan {
    fn qualified(&self) -> String {
        self.owners.join("::")
    }

    fn fact(&mut self, kind: &'static str, value: String, span: Span) {
        self.output.facts.push(Fact {
            qualified: self.qualified(),
            kind,
            value,
            test_only: self.test_only,
            location: span.into(),
        });
    }

    fn item(&mut self, kind: &'static str, signature: String, span: Span) {
        self.output.items.push(Item {
            qualified: self.qualified(),
            kind,
            signature,
            test_only: self.test_only,
            explicit_test_cfg: self.explicit_test_cfg,
            location: span.into(),
        });
    }

    fn enter(&mut self, name: String, attrs: &[syn::Attribute]) -> (bool, bool) {
        let previous = (self.test_only, self.explicit_test_cfg);
        self.owners.push(name);
        self.test_only |= facts::test_only(attrs);
        self.explicit_test_cfg = facts::explicit_test_cfg(attrs);
        previous
    }

    fn leave(&mut self, previous: (bool, bool)) {
        drop(self.owners.pop());
        (self.test_only, self.explicit_test_cfg) = previous;
    }
}

impl<'ast> Visit<'ast> for Scan {
    fn visit_visibility(&mut self, node: &'ast syn::Visibility) {
        if !matches!(node, syn::Visibility::Inherited) {
            self.fact("visibility", tokens(node), node.span());
        }
        visit::visit_visibility(self, node);
    }

    fn visit_item_macro(&mut self, node: &'ast syn::ItemMacro) {
        let name = node.ident.as_ref().map_or_else(
            || format!("macro({})", self.imports.path(&node.mac.path)),
            |name| format!("macro({name})"),
        );
        let previous = self.enter(name, &node.attrs);
        self.item("macro", self.imports.path(&node.mac.path), node.span());
        visit::visit_item_macro(self, node);
        self.leave(previous);
    }

    fn visit_item_mod(&mut self, node: &'ast syn::ItemMod) {
        let previous = self.enter(node.ident.to_string(), &node.attrs);
        self.item("module", node.ident.to_string(), node.span());
        let imports = self.imports.clone();
        if let Some((_, items)) = &node.content {
            self.imports = imports.with_items(items);
        }
        visit::visit_item_mod(self, node);
        self.imports = imports;
        self.leave(previous);
    }

    fn visit_block(&mut self, node: &'ast syn::Block) {
        let imports = self.imports.clone();
        self.imports = imports.with_block(node);
        visit::visit_block(self, node);
        self.imports = imports;
    }

    fn visit_signature(&mut self, node: &'ast syn::Signature) {
        if let Some(syn::punctuated::Pair::Punctuated(_, comma)) = node.inputs.pairs().next_back() {
            self.fact("parameter-trailing-comma", ",".to_owned(), comma.span());
        }
        visit::visit_signature(self, node);
    }

    fn visit_item_fn(&mut self, node: &'ast syn::ItemFn) {
        let previous = self.enter(node.sig.ident.to_string(), &node.attrs);
        self.item("function", tokens(&node.sig), node.span());
        visit::visit_item_fn(self, node);
        self.leave(previous);
    }

    fn visit_impl_item_fn(&mut self, node: &'ast syn::ImplItemFn) {
        let previous = self.enter(node.sig.ident.to_string(), &node.attrs);
        self.item("function", tokens(&node.sig), node.span());
        visit::visit_impl_item_fn(self, node);
        self.leave(previous);
    }

    fn visit_trait_item_fn(&mut self, node: &'ast syn::TraitItemFn) {
        let previous = self.enter(node.sig.ident.to_string(), &node.attrs);
        self.item("function", tokens(&node.sig), node.span());
        visit::visit_trait_item_fn(self, node);
        self.leave(previous);
    }

    fn visit_item_impl(&mut self, node: &'ast syn::ItemImpl) {
        let identity = if let Some((_, path, _)) = &node.trait_ {
            format!("<{} as {}>", tokens(&node.self_ty), self.imports.path(path))
        } else {
            tokens(&node.self_ty)
        };
        let previous = self.enter(identity, &node.attrs);
        self.item("impl", tokens(&node.self_ty), node.span());
        visit::visit_item_impl(self, node);
        self.leave(previous);
    }

    fn visit_item_struct(&mut self, node: &'ast syn::ItemStruct) {
        let previous = self.enter(node.ident.to_string(), &node.attrs);
        self.item("struct", node.ident.to_string(), node.span());
        visit::visit_item_struct(self, node);
        self.leave(previous);
    }

    fn visit_item_enum(&mut self, node: &'ast syn::ItemEnum) {
        let previous = self.enter(node.ident.to_string(), &node.attrs);
        self.item("enum", node.ident.to_string(), node.span());
        visit::visit_item_enum(self, node);
        self.leave(previous);
    }

    fn visit_item_trait(&mut self, node: &'ast syn::ItemTrait) {
        let previous = self.enter(node.ident.to_string(), &node.attrs);
        self.item("trait", node.ident.to_string(), node.span());
        visit::visit_item_trait(self, node);
        self.leave(previous);
    }

    fn visit_field(&mut self, node: &'ast syn::Field) {
        let name = node
            .ident
            .as_ref()
            .map_or_else(|| "<tuple-field>".to_owned(), ToString::to_string);
        let previous = self.enter(name, &node.attrs);
        self.item("field", tokens(&node.ty), node.span());
        self.fact("field-visibility", tokens(&node.vis), node.span());
        visit::visit_field(self, node);
        self.leave(previous);
    }

    fn visit_item_static(&mut self, node: &'ast syn::ItemStatic) {
        let previous = self.enter(node.ident.to_string(), &node.attrs);
        self.item("static", tokens(&node.ty), node.span());
        self.fact("static", tokens(&node.ty), node.span());
        visit::visit_item_static(self, node);
        self.leave(previous);
    }

    fn visit_item_const(&mut self, node: &'ast syn::ItemConst) {
        let previous = self.enter(node.ident.to_string(), &node.attrs);
        self.item("const", tokens(&node.ty), node.span());
        visit::visit_item_const(self, node);
        self.leave(previous);
    }

    fn visit_item_type(&mut self, node: &'ast syn::ItemType) {
        let previous = self.enter(node.ident.to_string(), &node.attrs);
        self.item("type", tokens(&node.ty), node.span());
        visit::visit_item_type(self, node);
        self.leave(previous);
    }

    fn visit_variant(&mut self, node: &'ast syn::Variant) {
        let previous = self.enter(node.ident.to_string(), &node.attrs);
        self.item("variant", node.ident.to_string(), node.span());
        visit::visit_variant(self, node);
        self.leave(previous);
    }

    fn visit_path(&mut self, node: &'ast syn::Path) {
        self.fact("path", self.imports.path(node), node.span());
        visit::visit_path(self, node);
    }

    fn visit_item_use(&mut self, node: &'ast syn::ItemUse) {
        self.fact("import", tokens(&node.tree), node.span());
        for target in self.imports.targets(&node.tree) {
            self.fact("import-target", target, node.span());
        }
        visit::visit_item_use(self, node);
    }

    fn visit_use_glob(&mut self, node: &'ast syn::UseGlob) {
        self.fact(
            "unresolved-glob",
            "compiler-resolved import; syntax cannot infer exports".to_owned(),
            node.span(),
        );
    }

    fn visit_attribute(&mut self, node: &'ast syn::Attribute) {
        attributes::record(self, "attribute", &node.meta);
        visit::visit_attribute(self, node);
    }

    fn visit_macro(&mut self, node: &'ast syn::Macro) {
        // Tokens in Rust macros are a DSL, not necessarily Rust expressions.
        // Emit them explicitly. Typed effect checks use compiler diagnostics;
        // the policy inventories unsupported item-producing macros separately.
        self.fact("macro", self.imports.path(&node.path), node.span());
        self.fact("macro-tokens", node.tokens.to_string(), node.span());
        macro_attributes::visit(self, node.tokens.clone());
        visit::visit_macro(self, node);
    }
}

#[cfg(test)]
mod tests;
