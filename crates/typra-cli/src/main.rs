use std::path::PathBuf;

use clap::{Parser, Subcommand};
use typra_core::catalog::decode_catalog_payload;
use typra_core::file_format::{decode_header, FILE_HEADER_SIZE};
use typra_core::segments::header::SegmentType;
use typra_core::segments::reader::{read_segment_payload, scan_segments, SegmentMeta};
use typra_core::storage::{FileStore, Store};
use typra_core::superblock::{decode_superblock, SUPERBLOCK_SIZE};

#[derive(Parser)]
#[command(name = "typra")]
#[command(about = "Operational CLI for Typra databases.", long_about = None)]
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
}

fn open_readonly_store(path: &PathBuf) -> Result<FileStore, typra_core::DbError> {
    let f = std::fs::OpenOptions::new().read(true).open(path)?;
    Ok(FileStore::new(f))
}

fn segment_start_offset() -> u64 {
    (FILE_HEADER_SIZE + 2 * SUPERBLOCK_SIZE) as u64
}

fn read_header_and_superblocks(
    store: &mut impl Store,
) -> Result<
    (
        typra_core::file_format::FileHeader,
        [u8; SUPERBLOCK_SIZE],
        [u8; SUPERBLOCK_SIZE],
    ),
    typra_core::DbError,
> {
    let len = store.len()?;
    if len < FILE_HEADER_SIZE as u64 {
        return Err(typra_core::DbError::Format(
            typra_core::FormatError::TruncatedHeader {
                got: len as usize,
                expected: FILE_HEADER_SIZE,
            },
        ));
    }

    let mut hdr_buf = [0u8; FILE_HEADER_SIZE];
    store.read_exact_at(0, &mut hdr_buf)?;
    let header = decode_header(&hdr_buf)?;

    let mut a = [0u8; SUPERBLOCK_SIZE];
    let mut b = [0u8; SUPERBLOCK_SIZE];
    store.read_exact_at(FILE_HEADER_SIZE as u64, &mut a)?;
    store.read_exact_at((FILE_HEADER_SIZE + SUPERBLOCK_SIZE) as u64, &mut b)?;

    Ok((header, a, b))
}

fn select_superblock(
    a: &[u8; SUPERBLOCK_SIZE],
    b: &[u8; SUPERBLOCK_SIZE],
) -> Option<typra_core::superblock::Superblock> {
    let sa = decode_superblock(a).ok();
    let sb = decode_superblock(b).ok();
    match (sa, sb) {
        (Some(sa), Some(sb)) => Some(if sa.generation >= sb.generation {
            sa
        } else {
            sb
        }),
        (Some(sa), None) => Some(sa),
        (None, Some(sb)) => Some(sb),
        (None, None) => None,
    }
}

fn load_catalog_from_segments(
    store: &mut impl Store,
    metas: &[SegmentMeta],
) -> Result<typra_core::Catalog, typra_core::DbError> {
    let mut cat = typra_core::Catalog::default();
    for meta in metas {
        if meta.header.segment_type != SegmentType::Schema {
            continue;
        }
        let payload = read_segment_payload(store, meta)?;
        let rec = decode_catalog_payload(&payload)?;
        cat.apply_record(rec)?;
    }
    Ok(cat)
}

fn inspect(path: PathBuf) -> Result<(), typra_core::DbError> {
    let mut store = open_readonly_store(&path)?;
    let (header, sb_a, sb_b) = read_header_and_superblocks(&mut store)?;
    let selected = select_superblock(&sb_a, &sb_b);

    println!("path: {}", path.display());
    println!("format: {}.{}", header.format_major, header.format_minor);

    match selected {
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

    // Catalog summary (decode schema segments only; no full replay required).
    let start = segment_start_offset();
    let metas = scan_segments(&mut store, start)?;
    let cat = load_catalog_from_segments(&mut store, &metas)?;
    println!("collections: {}", cat.collections().len());
    for c in cat.collections() {
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

fn verify(path: PathBuf) -> Result<(), typra_core::DbError> {
    let mut store = open_readonly_store(&path)?;
    let (header, _sb_a, _sb_b) = read_header_and_superblocks(&mut store)?;
    if store.len()? < segment_start_offset() {
        return Err(typra_core::DbError::Format(
            typra_core::FormatError::TruncatedSuperblock {
                got: store.len()? as usize,
                expected: segment_start_offset() as usize,
            },
        ));
    }

    // Segment framing + CRC (except checkpoint/temp payload policy) is validated by scan_segments.
    let metas = scan_segments(&mut store, segment_start_offset())?;

    // Also validate schema segments decode/apply.
    let _ = load_catalog_from_segments(&mut store, &metas)?;

    println!(
        "ok: format {}.{} segments={} schema_segments_ok=true",
        header.format_major,
        header.format_minor,
        metas.len()
    );
    Ok(())
}

fn dump_catalog(path: PathBuf) -> Result<(), typra_core::DbError> {
    let mut store = open_readonly_store(&path)?;
    let _ = read_header_and_superblocks(&mut store)?;
    let metas = scan_segments(&mut store, segment_start_offset())?;
    let cat = load_catalog_from_segments(&mut store, &metas)?;

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
    println!("{}", serde_json::to_string_pretty(&v).unwrap());
    Ok(())
}

fn main() {
    let cli = Cli::parse();
    let res = match cli.cmd {
        Command::Inspect { path } => inspect(path),
        Command::Verify { path } => verify(path),
        Command::DumpCatalog { path, .. } => dump_catalog(path),
    };
    if let Err(e) = res {
        eprintln!("error: {e:?}");
        std::process::exit(2);
    }
}
