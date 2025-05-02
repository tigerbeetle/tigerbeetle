# frozen_string_literal: true

require_relative "lib/tigerbeetle/version"

Gem::Specification.new do |spec|
  spec.name        = "tigerbeetle"
  spec.version     = TigerBeetle::VERSION
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
    "ext/tigerbeetle/*.{c,h,rb}",
    "ext/tigerbeetle/**/*.{so,dylib,dll}",  # Include precompiled libraries
    "README.md",
    "LICENSE",
    "*.gemspec"
  ])
  spec.bindir = "exe"
  spec.executables = spec.files.grep(%r{\Aexe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]
  spec.extensions = ['ext/tigerbeetle/extconf.rb']
end

