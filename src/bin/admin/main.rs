#[cfg(feature = "jemalloc")]
#[path = "../support/allocator.rs"]
mod allocator;

use std::fs::{self, File, OpenOptions as FileOpenOptions};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use anyhow::{Context, Result, bail};
use chimaera::{ExportOptions, ImportOptions, OpenOptions, Storage, VerificationOptions};
use clap::{Parser, Subcommand};
use serde::Serialize;
use serde_json::{Map, Value, json};
use uuid::Uuid;

const EX_OK: u8 = 0;
const EX_DATAERR: u8 = 65;
const EX_IOERR: u8 = 74;

#[derive(Debug, Parser)]
#[command(
    name = "chimaera-admin",
    version,
    about = "Chimaera storage administration"
)]
struct Arguments {
    #[arg(long, global = true)]
    json: bool,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Info {
        storage: PathBuf,
    },
    Verify {
        storage: PathBuf,
        #[arg(long, default_value_t = 4, value_parser = parse_verify_workers)]
        workers: usize,
    },
    Dump {
        storage: PathBuf,
        #[arg(long)]
        output: PathBuf,
        #[arg(long)]
        force: bool,
    },
    Load {
        dump: PathBuf,
        #[arg(long)]
        output: PathBuf,
    },
    Flush {
        storage: PathBuf,
        #[arg(long)]
        force: bool,
    },
    Checkpoint {
        storage: PathBuf,
        #[arg(long)]
        force: bool,
    },
    Compact {
        storage: PathBuf,
        #[arg(long)]
        force: bool,
    },
    /// Build an experimental query-only directory for an exact checkpoint.
    BuildPackDirectory {
        storage: PathBuf,
        #[arg(long)]
        output: PathBuf,
        #[arg(long)]
        force: bool,
    },
    Snapshot {
        storage: PathBuf,
        #[arg(long)]
        output: PathBuf,
    },
    Restore {
        snapshot: PathBuf,
        #[arg(long)]
        output: PathBuf,
    },
    Salvage {
        source: PathBuf,
        #[arg(long)]
        output: PathBuf,
    },
}

struct CommandOutput {
    operation: &'static str,
    fields: Value,
    human: String,
    exit_code: u8,
}

fn main() -> ExitCode {
    let arguments = Arguments::parse();
    match execute(&arguments.command) {
        Ok(output) => {
            if arguments.json {
                render_json(&output);
            } else {
                println!("{}", output.human);
            }
            ExitCode::from(output.exit_code)
        }
        Err(error) => {
            if arguments.json {
                println!(
                    "{}",
                    json!({
                        "ok": false,
                        "error": {
                            "code": "operation_failed",
                            "message": format!("{error:#}"),
                        }
                    })
                );
            } else {
                eprintln!("ERROR: {error:#}");
            }
            ExitCode::from(EX_IOERR)
        }
    }
}

fn execute(command: &Command) -> Result<CommandOutput> {
    #[cfg(feature = "jemalloc")]
    allocator::verify()?;
    match command {
        Command::Info { storage } => {
            let info = Storage::info(storage)?;
            success("info", &info, "storage information read")
        }
        Command::Verify { storage, workers } => {
            let report =
                Storage::verify_with_options(storage, VerificationOptions { workers: *workers });
            let valid = report.valid;
            let repair_needed = report.repair_needed;
            Ok(CommandOutput {
                operation: "verify",
                fields: serde_json::to_value(report)?,
                human: if valid {
                    if repair_needed {
                        "storage is valid; final WAL tail requires repair".into()
                    } else {
                        "storage is valid".into()
                    }
                } else {
                    "storage verification failed".into()
                },
                exit_code: if valid { EX_OK } else { EX_DATAERR },
            })
        }
        Command::Dump {
            storage,
            output,
            force,
        } => dump(storage, output, *force),
        Command::Load { dump, output } => load(dump, output),
        Command::Flush { storage, force } => {
            require_force(*force, "flush")?;
            let commit = with_storage(storage, false, Storage::flush)?;
            success(
                "flush",
                &json!({ "barrier_commit_id": commit.to_string() }),
                format!("flush completed at commit {commit}"),
            )
        }
        Command::Checkpoint { storage, force } => {
            require_force(*force, "checkpoint")?;
            let commit = with_storage(storage, false, Storage::checkpoint)?;
            success(
                "checkpoint",
                &json!({ "checkpoint_commit_id": commit.to_string() }),
                format!("checkpoint completed at commit {commit}"),
            )
        }
        Command::Compact { storage, force } => {
            require_force(*force, "compact")?;
            let segment = with_storage(storage, false, Storage::compact)?;
            success(
                "compact",
                &json!({ "last_segment_id": segment.to_string() }),
                format!("compaction completed at segment {segment}"),
            )
        }
        Command::BuildPackDirectory {
            storage,
            output,
            force,
        } => build_pack_directory(storage, output, *force),
        Command::Snapshot { storage, output } => {
            let snapshot = with_storage(storage, false, |storage| storage.snapshot(output))?;
            success(
                "snapshot",
                &snapshot,
                format!("snapshot created at {}", output.display()),
            )
        }
        Command::Restore { snapshot, output } => {
            let restored = Storage::restore_snapshot(snapshot, output)?;
            success(
                "restore",
                &restored,
                format!("snapshot restored at {}", output.display()),
            )
        }
        Command::Salvage { source, output } => {
            let salvaged = Storage::salvage(source, output)?;
            success(
                "salvage",
                &salvaged,
                format!("storage salvaged at {}", output.display()),
            )
        }
    }
}

fn parse_verify_workers(value: &str) -> std::result::Result<usize, String> {
    match value.parse::<usize>() {
        Ok(workers @ (1 | 2 | 4)) => Ok(workers),
        _ => Err("workers must be one of 1, 2, or 4".into()),
    }
}

fn dump(storage: &Path, output: &Path, force: bool) -> Result<CommandOutput> {
    validate_external_output(storage, output)?;
    if output.exists() && !force {
        bail!("dump output already exists; pass --force to replace it");
    }
    if output.is_dir() {
        bail!("dump output is a directory");
    }
    let temporary = sibling_temporary(output, "dump")?;
    let result: Result<chimaera::ExportResult> = (|| {
        let mut file = FileOpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)?;
        let exported = with_storage(storage, false, |storage| {
            storage.export_jsonl(
                &mut file,
                ExportOptions {
                    gzip: gzip_path(output),
                    ..ExportOptions::default()
                },
            )
        })?;
        file.sync_all()?;
        drop(file);
        fs::rename(&temporary, output)?;
        sync_parent(output)?;
        Ok(exported)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    let exported = result?;
    success(
        "dump",
        &exported,
        format!(
            "dump wrote {} points to {}",
            exported.points,
            output.display()
        ),
    )
}

fn load(dump: &Path, output: &Path) -> Result<CommandOutput> {
    if !dump.is_file() {
        bail!("dump file does not exist");
    }
    if output.exists() {
        bail!("load output already exists");
    }
    let temporary = sibling_temporary(output, "load")?;
    let result: Result<chimaera::ImportResult> = (|| {
        let storage = Storage::open(&temporary, OpenOptions::default())?;
        let file = File::open(dump)?;
        let imported = storage.import_jsonl(
            file,
            ImportOptions {
                gzip: gzip_path(dump),
                ..ImportOptions::default()
            },
        );
        let close = storage.close();
        let imported = imported?;
        close?;
        let report = Storage::verify(&temporary);
        if !report.valid || report.repair_needed {
            bail!("loaded storage failed final verification");
        }
        fs::rename(&temporary, output)?;
        sync_parent(output)?;
        Ok(imported)
    })();
    if result.is_err() {
        let _ = fs::remove_dir_all(&temporary);
    }
    let imported = result?;
    success(
        "load",
        &imported,
        format!(
            "load imported {} points into {}",
            imported.points_imported,
            output.display()
        ),
    )
}

fn build_pack_directory(storage: &Path, output: &Path, force: bool) -> Result<CommandOutput> {
    validate_external_output(storage, output)?;
    if output.is_dir() {
        bail!("pack directory output is a directory");
    }
    if output.exists() {
        if !force {
            bail!("pack directory output already exists; pass --force to replace it");
        }
        fs::remove_file(output)?;
    }
    let info = with_storage(storage, false, |storage| {
        storage.build_offline_pack_directory(output)
    })?;
    success(
        "build-pack-directory",
        &info,
        format!(
            "pack directory wrote {} locators to {}",
            info.block_locators,
            output.display()
        ),
    )
}

fn with_storage<T>(
    path: &Path,
    checkpoint_on_close: bool,
    operation: impl FnOnce(&Storage) -> chimaera::Result<T>,
) -> Result<T> {
    let info = Storage::info(path)?;
    let storage = Storage::open(
        path,
        OpenOptions {
            create_if_missing: false,
            checkpoint_on_close,
            virtual_shards: info.virtual_shards,
            partition_span_us: info.partition_span_us,
            ..OpenOptions::default()
        },
    )?;
    let result = operation(&storage);
    let close = storage.close();
    let value = result?;
    close?;
    Ok(value)
}

fn success(
    operation: &'static str,
    value: &impl Serialize,
    human: impl Into<String>,
) -> Result<CommandOutput> {
    Ok(CommandOutput {
        operation,
        fields: serde_json::to_value(value)?,
        human: human.into(),
        exit_code: EX_OK,
    })
}

fn render_json(output: &CommandOutput) {
    let mut object = match output.fields.clone() {
        Value::Object(object) => object,
        value => Map::from_iter([("result".into(), value)]),
    };
    object.insert("operation".into(), Value::String(output.operation.into()));
    object.insert("ok".into(), Value::Bool(output.exit_code == EX_OK));
    println!("{}", Value::Object(object));
}

fn require_force(force: bool, operation: &str) -> Result<()> {
    if !force {
        bail!("{operation} requires --force");
    }
    Ok(())
}

fn gzip_path(path: &Path) -> bool {
    path.extension().is_some_and(|extension| extension == "gz")
}

fn sibling_temporary(destination: &Path, operation: &str) -> Result<PathBuf> {
    let parent = destination
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let parent = fs::canonicalize(parent)
        .with_context(|| format!("output parent does not exist: {}", parent.display()))?;
    let name = destination
        .file_name()
        .context("output path must have a file name")?
        .to_string_lossy();
    Ok(parent.join(format!(".{name}.{operation}.{}.tmp", Uuid::new_v4())))
}

fn validate_external_output(source: &Path, destination: &Path) -> Result<()> {
    let source = fs::canonicalize(source)?;
    let parent = destination
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let parent = fs::canonicalize(parent)?;
    if parent.starts_with(source) {
        bail!("output must be outside the source storage");
    }
    Ok(())
}

fn sync_parent(path: &Path) -> Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    File::open(parent)?.sync_all()?;
    Ok(())
}
