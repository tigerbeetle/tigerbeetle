require_relative "tigerbeetle/version"
require_relative "tigerbeetle/bindings"
require_relative "tigerbeetle/id"
require_relative "tigerbeetle/completion_dispatcher"
require_relative "tigerbeetle/client"

native_extension = "tigerbeetle/tigerbeetle"

# Since we ship a fat binary gem, enable the bundled DLL path before loading the extension.
if Gem.win_platform?
  dll_path = File.expand_path("ext/tigerbeetle/lib/x86_64-windows", __dir__)

  begin
    require "ruby_installer/runtime"
    RubyInstaller::Runtime.add_dll_directory(dll_path) do
      require native_extension
    end
  rescue LoadError
    old_path = ENV["PATH"]
    ENV["PATH"] = "#{dll_path};#{old_path}"
    require native_extension
  ensure
    ENV["PATH"] = old_path if defined?(old_path)
  end
else
  require native_extension
end

module TigerBeetle
  private_constant :NativeClient
  private_constant :Request

  @id_generator = ID.new

  # Generates a 128-bit, time-based, monotonically increasing ID.
  def self.id
    @id_generator.generate
  end
end
