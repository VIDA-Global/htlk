//! Finite MCP inventory ingestion and exact selection.
use crate::executable::{
    cbor::{Limits, Value},
    digest::Digest,
};
use crate::{
    analyzer::{McpDescriptorKind, validate_mcp_descriptor},
    diagnostic::{Result, fail},
    executable::*,
    input_value::*,
};
use std::collections::{BTreeMap, btree_map::Entry};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum CatalogKind {
    Tool,
    Resource,
    Template,
    Prompt,
}

impl CatalogKind {
    fn collection(self) -> &'static str {
        match self {
            Self::Tool => "tools",
            Self::Resource => "resources",
            Self::Template => "templates",
            Self::Prompt => "prompts",
        }
    }

    fn descriptor_fields(self) -> (McpDescriptorKind, &'static str) {
        match self {
            Self::Tool => (McpDescriptorKind::Tool, "name"),
            Self::Resource => (McpDescriptorKind::Resource, "uri"),
            Self::Template => (McpDescriptorKind::ResourceTemplate, "uriTemplate"),
            Self::Prompt => (McpDescriptorKind::Prompt, "name"),
        }
    }
}

/// Validated, immutable snapshots of exactly three MCP catalogs.
#[derive(Clone, Debug)]
pub struct MCPCatalogInputs {
    bytes: usize,
    depth: usize,
    pub(crate) servers: BTreeMap<String, ServerIdentity>,
    entries: BTreeMap<(Digest, CatalogKind), BTreeMap<String, JsonDocument>>,
    pub(crate) schemas: BTreeMap<String, JsonDocument>,
}
impl MCPCatalogInputs {
    /// Parses the complete three-catalog JSON input, rejecting duplicate keys.
    ///
    /// # Errors
    /// Rejects malformed/unknown records, versions, conflicting aliases or entries,
    /// protocol-invalid descriptors, invalid retrieval roots, pins and size limits.
    pub fn from_json(bytes: &[u8], limits: &Limits) -> Result<Self> {
        let json = JsonDocument::new(bytes, limits).map_err(|e| fail("C_MCP", e))?;
        Self::from_value(json.value(), limits)
            .map(|mut result| {
                result.bytes = json.as_bytes().len();
                result.depth = value_depth(json.value());
                result
            })
            .map_err(|mut e| {
                for d in &mut e.diagnostics {
                    if d.code == "C_SYNTAX" {
                        d.code = "C_MCP".into();
                    }
                }
                e
            })
    }
    fn from_value(v: &Value, l: &Limits) -> Result<Self> {
        let root = closed(v, &["tools", "resources", "prompts"], &[])?;
        let mut result = Self {
            bytes: 0,
            depth: 0,
            servers: BTreeMap::new(),
            entries: BTreeMap::new(),
            schemas: BTreeMap::new(),
        };
        for (category, collections) in [
            ("tools", &[CatalogKind::Tool][..]),
            (
                "resources",
                &[CatalogKind::Resource, CatalogKind::Template][..],
            ),
            ("prompts", &[CatalogKind::Prompt][..]),
        ] {
            let mut names = vec![
                "catalog_version",
                "mcp_protocol_version",
                "servers",
                "schema_documents",
            ];
            names.extend(collections.iter().map(|kind| kind.collection()));
            let c = closed(field(root, category)?, &names, &[])?;
            if text(field(c, "catalog_version")?)? != "0.1"
                || text(field(c, "mcp_protocol_version")?)? != MCP_PROTOCOL_VERSION
            {
                return Err(fail("C_VERSION", "unsupported catalog or MCP version"));
            }
            let mut local = BTreeMap::new();
            for (alias, value) in map(field(c, "servers")?)?.iter() {
                if alias.is_empty() {
                    return Err(fail("C_MCP", "empty server alias"));
                }
                let server = ServerIdentity::from_value(value, l).map_err(|e| fail("C_MCP", e))?;
                if result.servers.get(alias).is_some_and(|s| s != &server) {
                    return Err(fail("C_MCP", "conflicting server alias"));
                }
                local.insert(alias, server.clone());
                result.servers.insert(alias.to_owned(), server);
            }
            for (uri, value) in map(field(c, "schema_documents")?)?.iter() {
                let doc = JsonDocument::from_value(value, l).map_err(|e| fail("C_SCHEMA", e))?;
                // Validate retrieval spelling and root shape without selecting a closure.
                crate::analyzer::SchemaResources::new(&doc, uri, l)
                    .map_err(|e| fail("C_SCHEMA", e))?;
                if result.schemas.get(uri).is_some_and(|d| d != &doc) {
                    return Err(fail("C_SCHEMA", "conflicting schema retrieval URI"));
                }
                result.schemas.insert(uri.to_owned(), doc);
            }
            for &kind in collections {
                let (protocol_kind, key) = kind.descriptor_fields();
                for value in array(field(c, kind.collection())?)? {
                    let e = closed(
                        value,
                        &["server", "descriptor"],
                        &["expected_descriptor_digest"],
                    )?;
                    let alias = text(field(e, "server")?)?;
                    let server = local
                        .get(alias)
                        .ok_or_else(|| fail("C_MCP", "unknown catalog server alias"))?;
                    let descriptor = JsonDocument::from_value(field(e, "descriptor")?, l)
                        .map_err(|e| fail("C_MCP", e))?;
                    validate_mcp_descriptor(protocol_kind, &descriptor, l)
                        .map_err(|e| fail("C_MCP", e))?;
                    if let Some(pin) = e.get("expected_descriptor_digest") {
                        let pin: Digest = text(pin)?.parse().map_err(|e| fail("C_MCP", e))?;
                        if pin != descriptor.digest() {
                            return Err(fail("C_MCP", "descriptor digest assertion mismatch"));
                        }
                    }
                    let selection = text(field(map(descriptor.value())?, key)?)?.to_owned();
                    let digest = server.digest(l).map_err(|e| fail("C_MCP", e))?;
                    match result
                        .entries
                        .entry((digest, kind))
                        .or_default()
                        .entry(selection)
                    {
                        Entry::Vacant(entry) => {
                            entry.insert(descriptor);
                        }
                        Entry::Occupied(entry) if entry.get() != &descriptor => {
                            return Err(fail("C_MCP", "conflicting operation descriptors"));
                        }
                        Entry::Occupied(_) => {}
                    }
                }
            }
        }
        Ok(result)
    }
    pub(crate) fn check_limits(&self, l: &Limits) -> Result<()> {
        if self.bytes > l.max_document_bytes || self.depth > l.max_depth {
            return Err(fail(
                "C_SOURCE_LIMIT",
                "catalog exceeds compiler input limits",
            ));
        }
        Ok(())
    }
    pub(crate) fn descriptor(&self, binding: &McpBinding, l: &Limits) -> Result<&JsonDocument> {
        let (kind, selection) = match binding.kind() {
            McpBindingKind::Tool { name, .. } => (CatalogKind::Tool, name),
            McpBindingKind::Resource { uri } => (CatalogKind::Resource, uri),
            McpBindingKind::Template { uri_template } => (CatalogKind::Template, uri_template),
            McpBindingKind::Prompt { name } => (CatalogKind::Prompt, name),
        };
        self.lookup(binding.server(), kind, selection, l)?
            .filter(|d| d.digest() == binding.descriptor())
            .ok_or_else(|| {
                fail(
                    "C_MCP",
                    "selected descriptor does not match supplied catalogs",
                )
            })
    }

    pub(crate) fn lookup(
        &self,
        server: &ServerIdentity,
        kind: CatalogKind,
        selection: &str,
        limits: &Limits,
    ) -> Result<Option<&JsonDocument>> {
        let digest = server.digest(limits).map_err(|e| fail("C_MCP", e))?;
        Ok(self
            .entries
            .get(&(digest, kind))
            .and_then(|entries| entries.get(selection)))
    }
}
fn value_depth(v: &Value) -> usize {
    match v {
        Value::Map(m) => m.iter().map(|(_, v)| 1 + value_depth(v)).max().unwrap_or(0),
        Value::Array(v) => v.iter().map(|v| 1 + value_depth(v)).max().unwrap_or(0),
        _ => 0,
    }
}
