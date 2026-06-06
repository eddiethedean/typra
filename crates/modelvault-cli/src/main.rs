use std::path::PathBuf;

use clap::{Parser, Subcommand};
use modelvault_core::db::{scan_database_file, DatabaseScanMode};

#[derive(Parser)]
#[command(name = "modelvault")]
#[command(about = "Operational CLI for ModelVault databases.", long_about = None)]
struct Cli {
    #[command(subcommand)]
    cmd: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Print format + recovery metadata + catalog summary.
    Inspect { path: PathBuf },
    /// Verify checksums/segment framing and ensure catalog segments decode.
    Verify { path: PathBuf },
    /// Dump the catalog as JSON.
    DumpCatalog {
        path: PathBuf,
        /// Emit JSON (default).
        #[arg(long, default_value_t = true)]
        json: bool,
    },
    /// Write a durable checkpoint to the database file.
    Checkpoint { path: PathBuf },
    /// Compact a database file.
    Compact {
        path: PathBuf,
        /// Compact in-place (atomic replace).
        #[arg(long, default_value_t = false)]
        in_place: bool,
        /// Compact to a new destination path.
        #[arg(long)]
        to: Option<PathBuf>,
    },
    /// Create a consistent backup snapshot to `--to` (checkpoint + copy).
    Backup {
        path: PathBuf,
        #[arg(long)]
        to: PathBuf,
        /// Verify the produced snapshot with `modelvault verify`.
        #[arg(long, default_value_t = false)]
        verify: bool,
    },
    /// Migration helpers (plan/apply).
    Migrate {
        #[command(subcommand)]
        cmd: MigrateCmd,
    },
}

#[derive(Subcommand)]
enum MigrateCmd {
    /// Plan a schema version bump and emit the required steps.
    Plan {
        path: PathBuf,
        #[arg(long)]
        collection: String,
        /// Path to a JSON file containing a `fields_json` array.
        #[arg(long)]
        schema_json: PathBuf,
        /// Optional path to a JSON file containing an `indexes_json` array.
        #[arg(long)]
        indexes_json: Option<PathBuf>,
    },
    /// Apply simple migration steps, then register the new schema version.
    Apply {
        path: PathBuf,
        #[arg(long)]
        collection: String,
        /// Path to a JSON file containing a `fields_json` array.
        #[arg(long)]
        schema_json: PathBuf,
        /// Optional path to a JSON file containing an `indexes_json` array.
        #[arg(long)]
        indexes_json: Option<PathBuf>,
        /// Backfill the given top-level field name (required for BackfillTopLevelField).
        #[arg(long)]
        backfill_field: Option<String>,
        /// JSON value to insert for backfill (e.g. `0`, `\"x\"`, `null`, `{...}`).
        #[arg(long)]
        backfill_value: Option<String>,
        /// Rebuild indexes after applying schema and/or before registering it.
        #[arg(long, default_value_t = false)]
        rebuild_indexes: bool,
        /// Force-register the schema version (bypass compatibility checks).
        #[arg(long, default_value_t = false)]
        force: bool,
    },
}

fn read_json_file(path: &PathBuf) -> Result<String, modelvault_core::DbError> {
    let s = std::fs::read_to_string(path)?;
    Ok(s)
}

fn parse_fields_json(s: &str) -> Result<Vec<modelvault_core::FieldDef>, modelvault_core::DbError> {
    // Accept the same "fields_json" shape as the Python API: list of {"path","type",...}.
    let v: serde_json::Value = serde_json::from_str(s)
        .map_err(|e| modelvault_core::DbError::Io(std::io::Error::other(e)))?;
    let arr = v.as_array().ok_or_else(|| {
        modelvault_core::DbError::Io(std::io::Error::other("schema_json must be a JSON array"))
    })?;
    let mut out = Vec::with_capacity(arr.len());
    for item in arr {
        let obj = item.as_object().ok_or_else(|| {
            modelvault_core::DbError::Io(std::io::Error::other("schema entry must be an object"))
        })?;
        let path_v = obj.get("path").ok_or_else(|| {
            modelvault_core::DbError::Io(std::io::Error::other("schema entry missing path"))
        })?;
        let path_arr = path_v.as_array().ok_or_else(|| {
            modelvault_core::DbError::Io(std::io::Error::other("path must be an array"))
        })?;
        let mut segs: Vec<std::borrow::Cow<'static, str>> = Vec::with_capacity(path_arr.len());
        for seg in path_arr {
            let s = seg.as_str().ok_or_else(|| {
                modelvault_core::DbError::Io(std::io::Error::other("path segment must be a string"))
            })?;
            segs.push(std::borrow::Cow::Owned(s.to_string()));
        }
        fn parse_type(
            v: &serde_json::Value,
        ) -> Result<modelvault_core::schema::Type, modelvault_core::DbError> {
            use modelvault_core::schema::Type;
            match v {
                serde_json::Value::String(s) => match s.as_str() {
                    "bool" => Ok(Type::Bool),
                    "int64" => Ok(Type::Int64),
                    "uint64" => Ok(Type::Uint64),
                    "float64" => Ok(Type::Float64),
                    "string" => Ok(Type::String),
                    "bytes" => Ok(Type::Bytes),
                    "uuid" => Ok(Type::Uuid),
                    "timestamp" => Ok(Type::Timestamp),
                    _ => Err(modelvault_core::DbError::Io(std::io::Error::other(
                        "unknown primitive type",
                    ))),
                },
                serde_json::Value::Object(m) => {
                    if let Some(inner) = m.get("optional") {
                        return Ok(Type::Optional(Box::new(parse_type(inner)?)));
                    }
                    if let Some(inner) = m.get("list") {
                        return Ok(Type::List(Box::new(parse_type(inner)?)));
                    }
                    if let Some(fields) = m.get("object") {
                        let arr = fields.as_array().ok_or_else(|| {
                            modelvault_core::DbError::Io(std::io::Error::other(
                                "object must be an array",
                            ))
                        })?;
                        let mut out: Vec<modelvault_core::FieldDef> = Vec::with_capacity(arr.len());
                        for item in arr {
                            let o = item.as_object().ok_or_else(|| {
                                modelvault_core::DbError::Io(std::io::Error::other(
                                    "object field entry must be an object",
                                ))
                            })?;
                            let path_v = o.get("path").ok_or_else(|| {
                                modelvault_core::DbError::Io(std::io::Error::other(
                                    "object field missing path",
                                ))
                            })?;
                            let path_arr = path_v.as_array().ok_or_else(|| {
                                modelvault_core::DbError::Io(std::io::Error::other(
                                    "path must be array",
                                ))
                            })?;
                            let mut segs: Vec<std::borrow::Cow<'static, str>> =
                                Vec::with_capacity(path_arr.len());
                            for seg in path_arr {
                                let s = seg.as_str().ok_or_else(|| {
                                    modelvault_core::DbError::Io(std::io::Error::other(
                                        "path segment must be string",
                                    ))
                                })?;
                                segs.push(std::borrow::Cow::Owned(s.to_string()));
                            }
                            let ty_v = o.get("type").ok_or_else(|| {
                                modelvault_core::DbError::Io(std::io::Error::other(
                                    "object field missing type",
                                ))
                            })?;
                            out.push(modelvault_core::FieldDef {
                                path: modelvault_core::schema::FieldPath(segs),
                                ty: parse_type(ty_v)?,
                                constraints: vec![],
                            });
                        }
                        return Ok(Type::Object(out));
                    }
                    if let Some(variants) = m.get("enum") {
                        let arr = variants.as_array().ok_or_else(|| {
                            modelvault_core::DbError::Io(std::io::Error::other(
                                "enum must be array",
                            ))
                        })?;
                        let mut vs: Vec<String> = Vec::with_capacity(arr.len());
                        for v in arr {
                            let s = v.as_str().ok_or_else(|| {
                                modelvault_core::DbError::Io(std::io::Error::other(
                                    "enum variant must be string",
                                ))
                            })?;
                            vs.push(s.to_string());
                        }
                        return Ok(Type::Enum(vs));
                    }
                    Err(modelvault_core::DbError::Io(std::io::Error::other(
                        "unknown composite type object",
                    )))
                }
                _ => Err(modelvault_core::DbError::Io(std::io::Error::other(
                    "type must be string or object",
                ))),
            }
        }

        fn parse_constraints(
            v: &serde_json::Value,
        ) -> Result<Vec<modelvault_core::schema::Constraint>, modelvault_core::DbError> {
            use modelvault_core::schema::Constraint;
            let arr = v.as_array().ok_or_else(|| {
                modelvault_core::DbError::Io(std::io::Error::other("constraints must be array"))
            })?;
            let mut out = Vec::with_capacity(arr.len());
            for item in arr {
                let obj = item.as_object().ok_or_else(|| {
                    modelvault_core::DbError::Io(std::io::Error::other("constraint must be object"))
                })?;
                if let Some(x) = obj.get("min_i64").and_then(|v| v.as_i64()) {
                    out.push(Constraint::MinI64(x));
                    continue;
                }
                if let Some(x) = obj.get("max_i64").and_then(|v| v.as_i64()) {
                    out.push(Constraint::MaxI64(x));
                    continue;
                }
                if let Some(x) = obj.get("min_u64").and_then(|v| v.as_u64()) {
                    out.push(Constraint::MinU64(x));
                    continue;
                }
                if let Some(x) = obj.get("max_u64").and_then(|v| v.as_u64()) {
                    out.push(Constraint::MaxU64(x));
                    continue;
                }
                if let Some(x) = obj.get("min_f64").and_then(|v| v.as_f64()) {
                    out.push(Constraint::MinF64(x));
                    continue;
                }
                if let Some(x) = obj.get("max_f64").and_then(|v| v.as_f64()) {
                    out.push(Constraint::MaxF64(x));
                    continue;
                }
                if let Some(x) = obj.get("min_length").and_then(|v| v.as_u64()) {
                    out.push(Constraint::MinLength(x));
                    continue;
                }
                if let Some(x) = obj.get("max_length").and_then(|v| v.as_u64()) {
                    out.push(Constraint::MaxLength(x));
                    continue;
                }
                if obj.get("email").and_then(|v| v.as_bool()) == Some(true) {
                    out.push(Constraint::Email);
                    continue;
                }
                if obj.get("url").and_then(|v| v.as_bool()) == Some(true) {
                    out.push(Constraint::Url);
                    continue;
                }
                if obj.get("nonempty").and_then(|v| v.as_bool()) == Some(true) {
                    out.push(Constraint::NonEmpty);
                    continue;
                }
                if let Some(r) = obj.get("regex").and_then(|v| v.as_str()) {
                    out.push(Constraint::Regex(r.to_string()));
                    continue;
                }
                return Err(modelvault_core::DbError::Io(std::io::Error::other(
                    "unknown constraint kind",
                )));
            }
            Ok(out)
        }

        let ty_v = obj.get("type").ok_or_else(|| {
            modelvault_core::DbError::Io(std::io::Error::other("schema entry missing type"))
        })?;
        let ty = parse_type(ty_v)?;
        let constraints = if let Some(c) = obj.get("constraints") {
            parse_constraints(c)?
        } else {
            vec![]
        };
        out.push(modelvault_core::FieldDef {
            path: modelvault_core::schema::FieldPath(segs),
            ty,
            constraints,
        });
    }
    Ok(out)
}

fn parse_indexes_json(
    s: &str,
) -> Result<Vec<modelvault_core::schema::IndexDef>, modelvault_core::DbError> {
    let v: serde_json::Value = serde_json::from_str(s)
        .map_err(|e| modelvault_core::DbError::Io(std::io::Error::other(e)))?;
    let arr = v.as_array().ok_or_else(|| {
        modelvault_core::DbError::Io(std::io::Error::other("indexes_json must be a JSON array"))
    })?;
    let mut out = Vec::with_capacity(arr.len());
    for item in arr {
        let obj = item.as_object().ok_or_else(|| {
            modelvault_core::DbError::Io(std::io::Error::other("index entry must be an object"))
        })?;
        let name = obj.get("name").and_then(|v| v.as_str()).ok_or_else(|| {
            modelvault_core::DbError::Io(std::io::Error::other("index missing name"))
        })?;
        let kind = obj.get("kind").and_then(|v| v.as_str()).ok_or_else(|| {
            modelvault_core::DbError::Io(std::io::Error::other("index missing kind"))
        })?;
        let kind = match kind {
            "unique" => modelvault_core::schema::IndexKind::Unique,
            "index" | "non_unique" => modelvault_core::schema::IndexKind::NonUnique,
            _ => {
                return Err(modelvault_core::DbError::Io(std::io::Error::other(
                    "index kind must be 'unique' or 'index'",
                )))
            }
        };
        let path_v = obj.get("path").ok_or_else(|| {
            modelvault_core::DbError::Io(std::io::Error::other("index missing path"))
        })?;
        let path_arr = path_v.as_array().ok_or_else(|| {
            modelvault_core::DbError::Io(std::io::Error::other("index path must be an array"))
        })?;
        let mut segs: Vec<std::borrow::Cow<'static, str>> = Vec::with_capacity(path_arr.len());
        for seg in path_arr {
            let s = seg.as_str().ok_or_else(|| {
                modelvault_core::DbError::Io(std::io::Error::other(
                    "index path segment must be string",
                ))
            })?;
            segs.push(std::borrow::Cow::Owned(s.to_string()));
        }
        out.push(modelvault_core::schema::IndexDef {
            name: name.to_string(),
            path: modelvault_core::schema::FieldPath(segs),
            kind,
        });
    }
    Ok(out)
}

fn migrate_plan(
    path: PathBuf,
    collection: String,
    schema_json: PathBuf,
    indexes_json: Option<PathBuf>,
) -> Result<(), modelvault_core::DbError> {
    let db = modelvault_core::Database::open_read_only(&path)?;
    let cid = db.collection_id_named(&collection)?;
    let fields = parse_fields_json(&read_json_file(&schema_json)?)?;
    let indexes = if let Some(p) = indexes_json {
        parse_indexes_json(&read_json_file(&p)?)?
    } else {
        vec![]
    };
    let plan = db.plan_schema_version_with_indexes(cid, fields, indexes)?;
    let v = serde_json::json!({
        "collection": collection,
        "change": format!("{:?}", plan.change),
        "steps": plan.steps.iter().map(|s| format!("{:?}", s)).collect::<Vec<_>>(),
    });
    let s = serde_json::to_string_pretty(&v)
        .map_err(|e| modelvault_core::DbError::Io(std::io::Error::other(e)))?;
    println!("{s}");
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn migrate_apply(
    path: PathBuf,
    collection: String,
    schema_json: PathBuf,
    indexes_json: Option<PathBuf>,
    backfill_field: Option<String>,
    backfill_value: Option<String>,
    rebuild_indexes: bool,
    force: bool,
) -> Result<(), modelvault_core::DbError> {
    let mut db = modelvault_core::Database::open(&path)?;
    let cid = db.collection_id_named(&collection)?;
    let fields = parse_fields_json(&read_json_file(&schema_json)?)?;
    let indexes = if let Some(p) = indexes_json {
        parse_indexes_json(&read_json_file(&p)?)?
    } else {
        vec![]
    };

    // Register the schema version first so engine helpers validate against the new schema.
    // If the update is `NeedsMigration`, use `--force` only after you've performed (or are about to
    // perform) the required rewrite steps.
    let ver = if force {
        db.register_schema_version_with_indexes_force(cid, fields, indexes)?
    } else {
        db.register_schema_version_with_indexes(cid, fields, indexes)?
    };

    // Best-effort apply requested steps (simple, explicit flags).
    if let (Some(field), Some(val)) = (backfill_field, backfill_value) {
        let v: serde_json::Value = serde_json::from_str(&val)
            .map_err(|e| modelvault_core::DbError::Io(std::io::Error::other(e)))?;
        fn row_value_from_json(v: &serde_json::Value) -> modelvault_core::RowValue {
            match v {
                serde_json::Value::Null => modelvault_core::RowValue::None,
                serde_json::Value::Bool(b) => modelvault_core::RowValue::Bool(*b),
                serde_json::Value::Number(n) => {
                    if let Some(i) = n.as_i64() {
                        modelvault_core::RowValue::Int64(i)
                    } else if let Some(u) = n.as_u64() {
                        modelvault_core::RowValue::Uint64(u)
                    } else if let Some(f) = n.as_f64() {
                        modelvault_core::RowValue::Float64(f)
                    } else {
                        modelvault_core::RowValue::None
                    }
                }
                serde_json::Value::String(s) => modelvault_core::RowValue::String(s.clone()),
                serde_json::Value::Array(a) => {
                    modelvault_core::RowValue::List(a.iter().map(row_value_from_json).collect())
                }
                serde_json::Value::Object(o) => {
                    let mut m = std::collections::BTreeMap::new();
                    for (k, vv) in o {
                        m.insert(k.clone(), row_value_from_json(vv));
                    }
                    modelvault_core::RowValue::Object(m)
                }
            }
        }
        let rv = row_value_from_json(&v);
        if field.contains('.') {
            use std::borrow::Cow;
            let path = modelvault_core::schema::FieldPath(
                field
                    .split('.')
                    .map(|s| Cow::Owned(s.to_string()))
                    .collect(),
            );
            db.backfill_field_at_path_with_value(cid, &path, rv)?;
        } else {
            db.backfill_top_level_field_with_value(cid, &field, rv)?;
        }
    }
    if rebuild_indexes {
        db.rebuild_indexes_for_collection(cid)?;
    }
    println!("ok: schema_version={}", ver.0);
    Ok(())
}

fn inspect(path: PathBuf) -> Result<(), modelvault_core::DbError> {
    let scan = scan_database_file(&path, DatabaseScanMode::Inspect)?;

    println!("path: {}", path.display());
    println!(
        "format: {}.{}",
        scan.header.format_major, scan.header.format_minor
    );

    match scan.superblock {
        None => {
            println!("superblock: none_valid");
            return Ok(());
        }
        Some(sb) => {
            println!("superblock_generation: {}", sb.generation);
            println!("manifest_offset: {}", sb.manifest_offset);
            println!("checkpoint_offset: {}", sb.checkpoint_offset);
            println!("checkpoint_len: {}", sb.checkpoint_len);
        }
    }

    println!("collections: {}", scan.catalog.collections().len());
    for c in scan.catalog.collections() {
        println!(
            "- {} (id={}, schema_version={}, indexes={})",
            c.name,
            c.id.0,
            c.current_version.0,
            c.indexes.len()
        );
    }
    Ok(())
}

fn verify(path: PathBuf) -> Result<(), modelvault_core::DbError> {
    let db = modelvault_core::Database::open_read_only(&path)?;
    db.verify_index_consistency()?;
    let scan = scan_database_file(&path, DatabaseScanMode::Verify)?;

    println!(
        "ok: format {}.{} segments={} schema_segments_ok=true indexes_ok=true",
        scan.header.format_major,
        scan.header.format_minor,
        scan.segments.len()
    );
    Ok(())
}

fn dump_catalog(path: PathBuf) -> Result<(), modelvault_core::DbError> {
    let scan = scan_database_file(&path, DatabaseScanMode::Verify)?;
    let cat = &scan.catalog;

    let v = serde_json::json!({
        "collections": cat.collections().iter().map(|c| {
            serde_json::json!({
                "id": c.id.0,
                "name": c.name,
                "current_version": c.current_version.0,
                "primary_field": c.primary_field,
                "fields": c.fields.iter().map(|f| {
                    serde_json::json!({
                        "path": f.path.0.iter().map(|s| s.as_ref()).collect::<Vec<_>>(),
                        "type": format!("{:?}", f.ty),
                        "constraints": f.constraints.iter().map(|k| format!("{k:?}")).collect::<Vec<_>>(),
                    })
                }).collect::<Vec<_>>(),
                "indexes": c.indexes.iter().map(|idx| {
                    serde_json::json!({
                        "name": idx.name,
                        "path": idx.path.0.iter().map(|s| s.as_ref()).collect::<Vec<_>>(),
                        "kind": format!("{:?}", idx.kind),
                    })
                }).collect::<Vec<_>>(),
            })
        }).collect::<Vec<_>>(),
    });
    let s = serde_json::to_string_pretty(&v)
        .map_err(|e| modelvault_core::DbError::Io(std::io::Error::other(e)))?;
    println!("{s}");
    Ok(())
}

fn checkpoint(path: PathBuf) -> Result<(), modelvault_core::DbError> {
    let mut db = modelvault_core::Database::open(&path)?;
    db.checkpoint()?;
    println!("ok: checkpoint written");
    Ok(())
}

fn compact(
    path: PathBuf,
    in_place: bool,
    to: Option<PathBuf>,
) -> Result<(), modelvault_core::DbError> {
    let mut db = modelvault_core::Database::open(&path)?;
    match (in_place, to) {
        (true, None) => {
            db.compact_in_place()?;
            println!("ok: compacted_in_place");
        }
        (false, Some(dest)) => {
            db.compact_to(dest)?;
            println!("ok: compacted_to");
        }
        (true, Some(_)) => {
            return Err(modelvault_core::DbError::Io(std::io::Error::other(
                "choose either --in-place or --to, not both",
            )));
        }
        (false, None) => {
            return Err(modelvault_core::DbError::Io(std::io::Error::other(
                "missing mode: pass --in-place or --to <dest>",
            )));
        }
    }
    Ok(())
}

fn backup(path: PathBuf, to: PathBuf, verify_after: bool) -> Result<(), modelvault_core::DbError> {
    let mut db = modelvault_core::Database::open(&path)?;
    db.export_snapshot_to_path(&to)?;
    println!("ok: backup_written path={}", to.display());
    if verify_after {
        verify(to)?;
    }
    Ok(())
}

fn main() {
    let cli = Cli::parse();
    let res = match cli.cmd {
        Command::Inspect { path } => inspect(path),
        Command::Verify { path } => verify(path),
        Command::DumpCatalog { path, .. } => dump_catalog(path),
        Command::Checkpoint { path } => checkpoint(path),
        Command::Compact { path, in_place, to } => compact(path, in_place, to),
        Command::Backup { path, to, verify } => backup(path, to, verify),
        Command::Migrate { cmd } => match cmd {
            MigrateCmd::Plan {
                path,
                collection,
                schema_json,
                indexes_json,
            } => migrate_plan(path, collection, schema_json, indexes_json),
            MigrateCmd::Apply {
                path,
                collection,
                schema_json,
                indexes_json,
                backfill_field,
                backfill_value,
                rebuild_indexes,
                force,
            } => migrate_apply(
                path,
                collection,
                schema_json,
                indexes_json,
                backfill_field,
                backfill_value,
                rebuild_indexes,
                force,
            ),
        },
    };
    if let Err(e) = res {
        eprintln!("error: {e:?}");
        std::process::exit(2);
    }
}
