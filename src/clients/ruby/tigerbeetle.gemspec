require_relative "src/tigerbeetle/version"

Gem::Specification.new do |spec|
  spec.name        = "tigerbeetle"
  spec.version     = TigerBeetle::VERSION
  spec.summary     = "The TigerBeetle client for Ruby."
  spec.authors     = ["TigerBeetle, Inc"]
  spec.license     = "Apache-2.0"
  spec.homepage    = "https://github.com/tigerbeetle/tigerbeetle"

  spec.metadata = {
    "source_code_uri" => "https://github.com/tigerbeetle/tigerbeetle",
    "bug_tracker_uri" => "https://github.com/tigerbeetle/tigerbeetle/issues",
  }

  spec.require_paths = ["src"]
  spec.extensions = ["src/ext/tigerbeetle/extconf.rb"]
  spec.files = Dir[
    "src/**/*.rb",
    "src/ext/tigerbeetle/extconf.rb",
    "src/ext/tigerbeetle/rb_tb_gen.h",
    "src/ext/tigerbeetle/tb_client.h",
    "src/ext/tigerbeetle/tigerbeetle.c",
    "src/ext/tigerbeetle/lib/**/*",
    "LICENSE",
    "README.md",
  ]

  spec.required_ruby_version = ">= 3.0"
end
