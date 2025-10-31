use std::{env, path::Path};

fn main() -> anyhow::Result<()> {
    let cargo_manifest_dir = env::var("CARGO_MANIFEST_DIR")?;

    if !Path::new(&format!("{cargo_manifest_dir}/assets/tb_client.h")).try_exists()? {
        panic!(
            "\n\
             TigerBeetle assets not found for in-tree build.\n\
             Run `zig/zig build clients:rust -Drelease` first.\n"
        );
    }

    assert!(Path::new(&format!("{cargo_manifest_dir}/src/tb_client.rs")).try_exists()?);

    println!("cargo:rerun-if-changed={cargo_manifest_dir}/assets/tb_client.h");

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
        ("x86_64", "windows", "msvc") => "x86_64-windows",
        _ => todo!(),
    };

    let libdir = format!("{libprefix}/{archpath}");
    let libname = "tb_client";

    println!("cargo:rustc-link-search=native={libdir}");
    println!("cargo:rustc-link-lib=static={libname}");

    let libfile = if unix {
        format!("lib{libname}.a")
    } else if windows {
        format!("{libname}.lib")
    } else {
        todo!()
    };
    let libpath = format!("{libdir}/{libfile}");

    assert!(Path::new(&libpath).try_exists()?);
    println!("cargo:rerun-if-changed={libpath}");

    if windows {
        // tb_client needs access to the random number generator in here.
        println!("cargo:rustc-link-lib=advapi32");
    }

    Ok(())
}
