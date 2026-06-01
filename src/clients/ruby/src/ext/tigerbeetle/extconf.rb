require "mkmf"
require "rbconfig"

arch = RbConfig::CONFIG["target_cpu"]
target_os = RbConfig::CONFIG["target_os"]

platform_arch =
  case arch
  when "x64", "x86_64"
    "x86_64"
  when "arm64", "aarch64"
    "aarch64"
  else
    raise "Unsupported architecture #{arch.inspect}"
  end

platform_os =
  case target_os
  when /linux/
    host_os = RbConfig::CONFIG["host_os"]

    if host_os.include?("musl") || target_os.include?("musl")
      "linux-musl"
    else
      "linux-gnu*"
    end
  when /darwin/
    "macos"
  when /mingw|mswin/
    raise "Unsupported Windows architecture #{arch.inspect}" unless platform_arch == "x86_64"

    "windows"
  else
    raise "Unsupported operating system #{target_os.inspect}"
  end

platform = "#{platform_arch}-#{platform_os}"

platform_dir = Dir.glob(File.join(__dir__, "lib", platform)).first
raise "No prebuilt libtb_client found for #{arch}-#{target_os}" unless platform_dir

$DLDFLAGS << " -Wl,-rpath,#{platform_dir}" if target_os.match?(/darwin/)

dir_config("tb_client", __dir__, platform_dir)
have_library("tb_client") or raise("libtb_client not found")

create_makefile("tigerbeetle/tigerbeetle")
