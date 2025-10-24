use ignore::Walk;
use std::{env, fs, path::Path, process::Command};

fn main() -> anyhow::Result<()> {
    let cargo_manifest_dir = env::var("CARGO_MANIFEST_DIR")?;

    prepare_dependencies(&cargo_manifest_dir)?;

    let unix = env::var("CARGO_CFG_UNIX").is_ok();
    let windows = env::var("CARGO_CFG_WINDOWS").is_ok();

    let target_arch = env::var("CARGO_CFG_TARGET_ARCH")?;
    let target_os = env::var("CARGO_CFG_TARGET_OS")?;
    let target_env = env::var("CARGO_CFG_TARGET_ENV")?;

    let target_arch = target_arch.as_ref();
    let target_os = target_os.as_ref();
    let target_env = target_env.as_ref();

    let libprefix = format!("{cargo_manifest_dir}/assets/lib");
    let archpath = match (target_arch, target_os, target_env) {
        ("aarch64", "linux", "gnu") => "aarch64-linux-gnu.2.27",
        ("aarch64", "linux", "musl") => "aarch64-linux-musl",
        ("aarch64", "macos", "") => "aarch64-macos",
        ("x86_64", "linux", "gnu") => "x86_64-linux-gnu.2.27",
        ("x86_64", "linux", "musl") => "x86_64-linux-musl",
        ("x86_64", "macos", "") => "x86_64-macos",
        ("x86_64", "windows", "") => "x86_64-windows",
        _ => todo!(),
    };

    let libdir = format!("{libprefix}/{archpath}");
    let libname = "tb_client";

    println!("cargo:rustc-link-search=native={libdir}");
    println!("cargo:rustc-link-lib=static={libname}");

    if unix {
        println!("cargo:rerun-if-changed={libdir}/lib{libname}.a");
    } else if windows {
        println!("cargo:rerun-if-changed={libdir}/{libname}.lib");
    } else {
        todo!();
    }

    Ok(())
}

fn prepare_dependencies(manifest_dir: &str) -> anyhow::Result<()> {
    let build_in_tree = is_build_in_tree(manifest_dir)?;
    if build_in_tree {
        build_tigerbeetle(manifest_dir)?;

        let tb_client_h = format!("{manifest_dir}/../c/tb_client.h");
        let tb_client_libs_dir = format!("{manifest_dir}/../c/lib");
        let tb_client_dest = format!("{manifest_dir}/assets");
        let tb_client_libs_dest = format!("{manifest_dir}/assets/lib");
        let tb_client_h_dest = format!("{manifest_dir}/assets/tb_client.h");

        fs::create_dir_all(&tb_client_dest)?;
        fs::copy(&tb_client_h, tb_client_h_dest)?;

        copy_dir_recursive(
            Path::new(&tb_client_libs_dir),
            Path::new(&tb_client_libs_dest),
        )?;
        emit_tigerbeetle_rerun_if_changed(manifest_dir)?;

        Ok(())
    } else {
        todo!()
    }
}

fn is_build_in_tree(manifest_dir: &str) -> anyhow::Result<bool> {
    Ok(fs::exists(format!("{manifest_dir}/../../../build.zig"))?)
}

fn build_tigerbeetle(manifest_dir: &str) -> anyhow::Result<()> {
    assert!(is_build_in_tree(manifest_dir)?);

    let tigerbeetle_root = format!("{manifest_dir}/../../..");
    let zig_compiler = if cfg!(unix) {
        let mut zig_path = format!("{tigerbeetle_root}/zig/zig");
        if !fs::exists(&zig_path)? {
            // Fallback to the system zig if installed
            match Command::new("which").arg("zig").output() {
                Ok(output) => {
                    if output.status.success() {
                        let path = String::from_utf8_lossy(&output.stdout).trim().to_string();
                        if !path.is_empty() {
                            zig_path = path;
                        }
                    }
                }
                Err(_) => {}
            }
        }
        zig_path
    } else if cfg!(windows) {
        format!("{tigerbeetle_root}/zig/zig.exe")
    } else {
        todo!()
    };

    if !fs::exists(&zig_compiler)? {
        println!("cargo:warning=No zig compiler found at {zig_compiler}.");
        println!("cargo:warning=You may need to run zig/download.ps1.");
        panic!("No zig compiler found.");
    }

    let build_targets = [
        "clients:c",    // Build the tb_client library and tb_client.h
        "clients:rust", // Build the tb_client library and tb_client.rs
        "install",      // Build tigerbeetle binary for testing
    ];

    for build_target in build_targets {
        let mut cmd = std::process::Command::new(&zig_compiler);
        cmd.args(["build", build_target, "-Drelease"]);
        let result = cmd.status()?;

        if !result.success() {
            panic!("zig build failed");
        }
    }

    Ok(())
}

fn emit_tigerbeetle_rerun_if_changed(manifest_dir: &str) -> anyhow::Result<()> {
    let tigerbeetle_root = format!("{manifest_dir}/../../..");
    for entry in Walk::new(&tigerbeetle_root) {
        let entry = entry?;
        if let Some(ext) = entry.path().extension() {
            if ext == "zig" {
                println!("cargo:rerun-if-changed={}", entry.path().display());
            }
        }
    }

    Ok(())
}

fn copy_dir_recursive(src: &Path, dst: &Path) -> anyhow::Result<()> {
    for entry in Walk::new(src) {
        let entry = entry?;
        let relative_path = entry.path().strip_prefix(src)?;
        let target_path = dst.join(relative_path);

        if entry.file_type().map(|ft| ft.is_dir()).unwrap_or(false) {
            fs::create_dir_all(&target_path)?;
        } else {
            fs::copy(entry.path(), &target_path)?;
        }
    }
    Ok(())
}
