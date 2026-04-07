require "mkmf"
require "rbconfig"

arch = RbConfig::CONFIG["target_cpu"]
os = RbConfig::CONFIG["target_os"]

platform_dir = Dir.glob(File.join(__dir__, "lib", "#{arch}-#{os.split("-").first}*")).first
raise "No prebuilt libtb_client found for #{arch}-#{os}" unless platform_dir

dir_config("tb_client", __dir__, platform_dir)
have_library("tb_client") or raise("libtb_client not found")

create_makefile("tigerbeetle/tigerbeetle")
