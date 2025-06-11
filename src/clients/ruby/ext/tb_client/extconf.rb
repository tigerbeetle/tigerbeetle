require "mkmf"
require "rbconfig"

def detect_platform
  host_os = RbConfig::CONFIG['host_os']
  cpu = RbConfig::CONFIG['host_cpu']

  # Normalize CPU architecture names
  cpu = case cpu
  when /x86_64|x64|amd64/i
    'x86_64'
  when /aarch64|arm64/i
    'aarch64'
  else
    cpu
  end

  # Detect OS and specific variant
  if host_os =~ /darwin|mac os/i
    "#{cpu}-macos"
  elsif host_os =~ /mingw|mswin|windows/i
    "#{cpu}-windows"
  elsif host_os =~ /linux/i
    # Check for musl vs glibc
    if system('ldd --version 2>&1 | grep -q musl')
      "#{cpu}-linux-musl"
    else
      # For glibc systems, try to get the version
      glibc_version = `ldd --version 2>&1 | head -n 1`
      if glibc_version =~ /2\.27/
        "#{cpu}-linux-gnu.2.27"
      else
        "#{cpu}-linux-gnu.2.27" # Default to 2.27 if can't determine version
      end
    end
  else
    nil # Unknown platform
  end
end

platform_dir = detect_platform
if platform_dir.nil?
  abort "Unsupported platform: #{RbConfig::CONFIG['host_os']} / #{RbConfig::CONFIG['host_cpu']}"
end

STDERR.puts "Detected platform: #{platform_dir}"

inc_dir = File.join(File.expand_path(File.dirname(__FILE__)), "..", "rb_tigerbeetle")
lib_dir = File.join(inc_dir, platform_dir)
abort "Lib dir missing" unless Dir.exist?(lib_dir)

lib_file = if platform_dir.include?('macos')
  File.join(lib_dir, 'librb_tigerbeetle.dylib')
elsif platform_dir.include?('windows')
  File.join(lib_dir, 'rb_tigerbeetle.dll')
else
  File.join(lib_dir, 'librb_tigerbeetle.so')
end

abort "#{lib_file} not found" unless File.exist?(lib_file)

dir_config("rb_tigerbeetle", inc_dir, lib_dir)

# have_header("tb_bindings.h", nil, inc_dir) or abort "tb_bindings.h not found"
have_library("rb_tigerbeetle", "initialize_ruby_client", "rb_tigerbeetle.h") or abort "rb_tigerbeetle library not found"

append_ldflags("-Wl,-rpath,@loader_path") if platform_dir.include?("macos")

create_makefile("tb_client/tb_client")

if platform_dir.include?("macos")
  modified_lines = File.readlines("Makefile").map do |line|
    if line.start_with?("install:")
      line.strip + " install-dylib\n"
    else
      line
    end
  end

  # Note Makefile requires a tab character at the beginning of the line
  modified_lines << <<~INSTALL_DYLIB

  install-dylib:
  \t$(INSTALL_DATA) #{lib_file} $(RUBYARCHDIR)

  # Preprocessing target
  tigerbeetle.i: tigerbeetle.c
  \t$(CC) $(CFLAGS) $(CPPFLAGS) $(DEFS) -E $< > $@

  preprocess: tigerbeetle.i
  \t@echo "Preprocessed file created: tigerbeetle.i"

  INSTALL_DYLIB

  File.write("Makefile", modified_lines.join)
end
