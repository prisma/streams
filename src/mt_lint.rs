//! SR-6: the multitenancy identity lint.
//!
//! Syn-based, in-suite, and ZERO-residual for the security categories
//! the shared-cell review named — unlike `scripts/multitenancy-audit.sh`
//! (which stays, as the fingerprint inventory for the identity-neutral
//! categories: stream-hash callers, internal wire headers, tenant-env
//! fallbacks), this lint parses the crate and FAILS the suite on any
//! unmarked site. There is no baseline to regenerate: a new site either
//! doesn't compile the pattern, or carries an inline
//! `// mt-lint: allow(<category>): <reason>` marker on its own or the
//! preceding line — a reviewed exemption visible in the diff.
//!
//! Categories (docs/MULTITENANCY.md §18 Stage 8; the review's rule:
//! "once a request or background task has a TenantStreamRef, no
//! shared-core function may reconstruct that identity from a bare
//! name"):
//!
//! * `raw-adapter-sref` — `DeploymentIdentity::raw_adapter_sref` is callable
//!   ONLY from the raw-surface adapters (`get_segments`,
//!   `stream_entry_inner`, `read` in src/http.rs); the single other
//!   sanctioned site is billing's pre-Stage-7 legacy-row fallback,
//!   which carries a marker.
//! * `name-keyed-map` — map/set fields and statics keyed by bare
//!   `String`/`Arc<str>` (tuple keys included). Stream identity must
//!   key by `TenantStreamRef`; every non-identity String map carries a
//!   marker naming what the key actually is.
//! * `stream-ref-construction` — `.stream_ref(` builds identity from a
//!   name, so it belongs at ingress (http.rs, product.rs, tenant.rs).
//!   Elsewhere: marker required (e.g. billing's system-ledger refs).
//! * `name-param-shared-core` — shared-core fn signatures must not
//!   take `name: &str`-shaped stream identity; markers for the
//!   reviewed identity-neutral helpers (shape predicates, key
//!   builders that already carry the project).
//! * `ops-event-literal` — `OpsEvent` is constructed ONLY through its
//!   builders (ops.rs), whose `stream()` takes a `TenantStreamRef` —
//!   the type system, not this lint, forces the project onto customer
//!   events. Literal construction elsewhere would bypass it. No
//!   markers.
//! * `dual-identity-params` — no fn takes BOTH a `TenantStreamRef`
//!   and a bare name param; the review's "remove dual (sref, name)
//!   params". No markers.
//! * `state-tenant-read` — reading the deployment tenant (any `.tenant`
//!   field, or `DeploymentIdentity::deployment_tenant()`) IS adopting the
//!   deployment tenant's identity; it is sanctioned only inside
//!   `raw_adapter_sref` itself. Every other read carries a marker
//!   naming its posture, so `state.tenant.stream_ref(name)` can never
//!   slip into a product helper unmarked, even in the ingress files.
//!
//! Scope: src/*.rs. Excluded: src/dst.rs + src/dst/ (the DST harness
//! and tests — fixture staging legitimately uses raw identity),
//! src/bin/ (single-tenant client tools), this file, and every
//! `#[cfg(test)]` item in scanned files.

use quote::ToTokens;
use syn::visit::Visit;

const SCAN_SKIP: &[&str] = &["dst.rs", "mt_lint.rs"];

#[derive(Debug)]
struct Violation {
    file: String,
    line: usize,
    category: &'static str,
    what: String,
}

struct Lint<'a> {
    file: String,
    lines: Vec<&'a str>,
    fn_stack: Vec<String>,
    violations: Vec<Violation>,
    markers: Vec<(String, usize, &'static str)>,
}

fn norm_type(t: &impl ToTokens) -> String {
    t.to_token_stream()
        .to_string()
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect()
}

const NAME_KEYED: &[&str] = &[
    "HashMap<String",
    "HashMap<(String",
    "HashMap<Arc<str>",
    "HashMap<(Arc<str>",
    "BTreeMap<String",
    "BTreeMap<(String",
    "BTreeMap<Arc<str>",
    "HashSet<String",
    "BTreeSet<String",
];

const NAME_PARAMS: &[&str] = &["name", "stream_name", "canonical_name", "stream"];

/// Ingress files where `.stream_ref(` construction is sanctioned: the
/// verified principal (product) and the deployment tenant (raw
/// adapters) both live behind route handlers here, and tenant.rs owns
/// the constructor.
const STREAM_REF_FILES: &[&str] = &["http.rs", "product.rs", "tenant.rs"];

/// Ingress/surface files exempt from the shared-core name-param rule —
/// route handlers legitimately receive the path's name string before
/// qualifying it.
const SURFACE_FILES: &[&str] = &["http.rs", "product.rs", "operator.rs", "tenant.rs"];

/// The one sanctioned home of `raw_adapter_sref` calls: (file, fn).
const RAW_ADAPTER_FNS: &[&str] = &["get_segments", "stream_entry_inner", "read"];

impl<'a> Lint<'a> {
    fn marker(&mut self, line: usize, cat: &'static str) -> bool {
        let m = format!("mt-lint: allow({cat})");
        let idx = line.saturating_sub(1);
        // Same line, else walk up through the contiguous comment /
        // attribute block directly above the item (doc comments and
        // serde attrs may sit between the marker and the flagged line).
        let mut hit = self.lines.get(idx).is_some_and(|l| l.contains(&m));
        let mut i = idx;
        while !hit && i > 0 {
            i -= 1;
            let t = self.lines[i].trim_start();
            if t.starts_with("//") {
                hit = t.contains(&m);
            } else if !(t.starts_with("#[") || t.starts_with("#!")) {
                break;
            }
        }
        if hit {
            self.markers.push((self.file.clone(), line, cat));
        }
        hit
    }

    fn flag(&mut self, line: usize, category: &'static str, what: String) {
        self.violations.push(Violation {
            file: self.file.clone(),
            line,
            category,
            what,
        });
    }

    fn check_map_type(&mut self, line: usize, ty: &str, ctx: &str) {
        if NAME_KEYED.iter().any(|p| ty.contains(p)) && !self.marker(line, "name-keyed-map") {
            self.flag(line, "name-keyed-map", format!("{ctx}: {ty}"));
        }
    }

    fn check_signature(&mut self, line: usize, name: &str, sig: &syn::Signature) {
        let mut has_sref_param = false;
        let mut name_params: Vec<(String, String)> = Vec::new();
        for input in &sig.inputs {
            if let syn::FnArg::Typed(pt) = input {
                let ty = norm_type(&pt.ty);
                if ty.contains("TenantStreamRef") {
                    has_sref_param = true;
                }
                if let syn::Pat::Ident(pi) = &*pt.pat {
                    let pname = pi.ident.to_string();
                    if NAME_PARAMS.contains(&pname.as_str())
                        && (ty.contains("str") || ty == "String" || ty.contains("<String>"))
                    {
                        name_params.push((pname, ty));
                    }
                }
            }
        }
        if has_sref_param && !name_params.is_empty() {
            self.flag(
                line,
                "dual-identity-params",
                format!("fn {name} takes TenantStreamRef AND {:?}", name_params),
            );
        } else if !name_params.is_empty()
            && !SURFACE_FILES.contains(&self.file.as_str())
            && !self.marker(line, "name-param-shared-core")
        {
            self.flag(
                line,
                "name-param-shared-core",
                format!("fn {name} takes {:?}", name_params),
            );
        }
    }
}

fn is_test_gated(attrs: &[syn::Attribute]) -> bool {
    attrs.iter().any(|a| {
        let s = a.to_token_stream().to_string();
        (s.contains("cfg") && s.contains("test")) || s.contains("# [test]") || s.contains("::test")
    })
}

impl<'a, 'ast> Visit<'ast> for Lint<'a> {
    fn visit_item_mod(&mut self, m: &'ast syn::ItemMod) {
        if is_test_gated(&m.attrs) {
            return;
        }
        syn::visit::visit_item_mod(self, m);
    }

    fn visit_item_fn(&mut self, f: &'ast syn::ItemFn) {
        if is_test_gated(&f.attrs) {
            return;
        }
        let name = f.sig.ident.to_string();
        self.check_signature(f.sig.ident.span().start().line, &name, &f.sig);
        self.fn_stack.push(name);
        syn::visit::visit_item_fn(self, f);
        self.fn_stack.pop();
    }

    fn visit_impl_item_fn(&mut self, f: &'ast syn::ImplItemFn) {
        if is_test_gated(&f.attrs) {
            return;
        }
        let name = f.sig.ident.to_string();
        self.check_signature(f.sig.ident.span().start().line, &name, &f.sig);
        self.fn_stack.push(name);
        syn::visit::visit_impl_item_fn(self, f);
        self.fn_stack.pop();
    }

    fn visit_field(&mut self, field: &'ast syn::Field) {
        let ty = norm_type(&field.ty);
        let line = field
            .ident
            .as_ref()
            .map(|i| i.span().start().line)
            .unwrap_or_else(|| field.ty.span_line());
        let ctx = field
            .ident
            .as_ref()
            .map(|i| format!("field {i}"))
            .unwrap_or_else(|| "tuple field".into());
        self.check_map_type(line, &ty, &ctx);
        syn::visit::visit_field(self, field);
    }

    fn visit_item_static(&mut self, s: &'ast syn::ItemStatic) {
        let ty = norm_type(&s.ty);
        self.check_map_type(
            s.ident.span().start().line,
            &ty,
            &format!("static {}", s.ident),
        );
        syn::visit::visit_item_static(self, s);
    }

    fn visit_expr_method_call(&mut self, c: &'ast syn::ExprMethodCall) {
        let m = c.method.to_string();
        let line = c.method.span().start().line;
        if m == "raw_adapter_sref" {
            let ok = self.file == "http.rs"
                && self
                    .fn_stack
                    .iter()
                    .any(|f| RAW_ADAPTER_FNS.contains(&f.as_str()));
            if !ok && !self.marker(line, "raw-adapter-sref") {
                self.flag(
                    line,
                    "raw-adapter-sref",
                    format!(
                        "raw_adapter_sref outside the raw adapters (in {})",
                        self.fn_stack.last().map(|s| s.as_str()).unwrap_or("?")
                    ),
                );
            }
        } else if m == "deployment_tenant"
            && !self.fn_stack.iter().any(|n| n == "raw_adapter_sref")
            && !self.marker(line, "state-tenant-read")
        {
            // PR 6-D: the accessor on the identity owner is the field it
            // replaced — adopting the deployment tenant stays a reviewed act.
            self.flag(
                line,
                "state-tenant-read",
                format!(
                    "deployment-tenant identity adopted (in {})",
                    self.fn_stack.last().map(|s| s.as_str()).unwrap_or("?")
                ),
            );
        } else if m == "stream_ref"
            && !STREAM_REF_FILES.contains(&self.file.as_str())
            && !self.marker(line, "stream-ref-construction")
        {
            self.flag(
                line,
                "stream-ref-construction",
                format!(
                    "identity constructed from a name outside ingress (in {})",
                    self.fn_stack.last().map(|s| s.as_str()).unwrap_or("?")
                ),
            );
        }
        syn::visit::visit_expr_method_call(self, c);
    }

    fn visit_expr_field(&mut self, f: &'ast syn::ExprField) {
        if let syn::Member::Named(m) = &f.member
            && m == "tenant"
        {
            // Round-3: RECEIVER-BLIND — any field access named `tenant`
            // counts, whatever the variable is called (`app.tenant`,
            // `ctx.tenant`, ...). The legitimate set is tiny and every
            // member carries a marker.
            if !self.fn_stack.iter().any(|n| n == "raw_adapter_sref")
                && !self.marker(m.span().start().line, "state-tenant-read")
            {
                self.flag(
                    m.span().start().line,
                    "state-tenant-read",
                    format!(
                        "deployment-tenant identity adopted (in {})",
                        self.fn_stack.last().map(|s| s.as_str()).unwrap_or("?")
                    ),
                );
            }
        }
        syn::visit::visit_expr_field(self, f);
    }

    fn visit_expr_struct(&mut self, e: &'ast syn::ExprStruct) {
        if self.file != "ops.rs"
            && e.path
                .segments
                .last()
                .is_some_and(|s| s.ident == "OpsEvent")
        {
            self.flag(
                e.path.segments.last().unwrap().ident.span().start().line,
                "ops-event-literal",
                "OpsEvent literal bypasses the project-typed builder".into(),
            );
        }
        syn::visit::visit_expr_struct(self, e);
    }
}

trait SpanLine {
    fn span_line(&self) -> usize;
}
impl SpanLine for syn::Type {
    fn span_line(&self) -> usize {
        use syn::spanned::Spanned;
        self.span().start().line
    }
}

#[test]
fn multitenancy_identity_lint() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut violations = Vec::new();
    let mut markers = Vec::new();
    let mut scanned = 0usize;
    // Recursive: new submodules are scanned the day they appear.
    // src/dst/ (harness + tests) and src/bin/ (single-tenant client
    // tools) stay out by directory.
    fn walk(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
        for e in std::fs::read_dir(dir).unwrap().filter_map(|e| e.ok()) {
            let p = e.path();
            let name = p.file_name().unwrap().to_str().unwrap().to_string();
            if p.is_dir() {
                if name != "dst" && name != "bin" {
                    walk(&p, out);
                }
            } else if p.extension().is_some_and(|x| x == "rs")
                && !SCAN_SKIP.contains(&name.as_str())
            {
                out.push(p);
            }
        }
    }
    let mut entries: Vec<std::path::PathBuf> = Vec::new();
    walk(&root, &mut entries);
    entries.sort();
    for path in entries {
        let src = std::fs::read_to_string(&path).unwrap();
        let ast = match syn::parse_file(&src) {
            Ok(a) => a,
            Err(e) => panic!("mt-lint: cannot parse {}: {e}", path.display()),
        };
        // A file-module whose INNER attribute is #![cfg(test)] is the
        // file-split form of `#[cfg(test)] mod` — same gate. (The
        // declaring `mod` line's outer attr lives in another file,
        // invisible to this per-file walk.)
        if is_test_gated(&ast.attrs) {
            continue;
        }
        let mut lint = Lint {
            file: path.file_name().unwrap().to_str().unwrap().to_string(),
            lines: src.lines().collect(),
            fn_stack: Vec::new(),
            violations: Vec::new(),
            markers: Vec::new(),
        };
        lint.visit_file(&ast);
        violations.extend(lint.violations);
        markers.extend(lint.markers);
        scanned += 1;
    }
    // The reviewed-exemption inventory: every marker is visible in the
    // suite output, so certification can eyeball the full list.
    let mut by_cat: std::collections::BTreeMap<&str, usize> = std::collections::BTreeMap::new();
    for (_, _, c) in &markers {
        *by_cat.entry(c).or_default() += 1;
    }
    println!(
        "mt-lint: {scanned} files, {} reviewed exemption markers: {:?}",
        markers.len(),
        by_cat
    );
    for (f, l, c) in &markers {
        println!("mt-lint:   allow({c}) {f}:{l}");
    }
    if !violations.is_empty() {
        for v in &violations {
            eprintln!(
                "mt-lint: VIOLATION [{}] src/{}:{} — {}",
                v.category, v.file, v.line, v.what
            );
        }
        panic!(
            "mt-lint: {} unmarked identity violation(s); convert to \
             TenantStreamRef identity or add a reviewed \
             `// mt-lint: allow(<category>): <reason>` marker",
            violations.len()
        );
    }
}
