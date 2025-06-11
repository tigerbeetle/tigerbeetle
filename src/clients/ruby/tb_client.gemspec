# frozen_string_literal: true

require_relative "lib/tb_client/version"

Gem::Specification.new do |spec|
  spec.name        = "tb_client"
  spec.version     = TBClient::VERSION
  spec.summary     = "TigerBeetle Ruby Client"
  spec.description = "A Ruby client for the TigerBeetle database, a high-performance, fault-tolerant, and scalable database designed for financial transactions."
  spec.homepage    = "https://github.com/yourusername/tigerbeetle-ruby" # Replace with your actual repo
  spec.license     = "MIT"
  spec.required_ruby_version = ">= 3.2.0"

  spec.authors     = ["Trevor John"]
  spec.email       = ["your.email@example.com"] # Add your email

  # Only use one file specification method - using Dir.glob is more reliable
  spec.files = Dir.glob([
    "lib/**/*.rb",
    "ext/tb_client/*.{c,h,rb}",
    "ext/tb_client/**/*.{so,dylib,dll}",  # Include precompiled libraries
    "README.md",
    "LICENSE",
    "*.gemspec"
  ])
  spec.bindir = "exe"
  spec.executables = spec.files.grep(%r{\Aexe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]
  spec.extensions = ['ext/tb_client/extconf.rb']
end

