# frozen_string_literal: true

require_relative "lib/tigerbeetle/version"

Gem::Specification.new do |spec|
  spec.name        = "tigerbeetle"
  spec.version     = TigerBeetle::VERSION
  spec.summary     = "TigerBeetle Ruby Client"
  spec.description = "A Ruby client for the TigerBeetle database, a high-performance, fault-tolerant, and scalable database designed for financial transactions."
  spec.summary = "TODO: Write a short summary, because RubyGems requires one."
  spec.description = "TODO: Write a longer description or delete this line."
  spec.homepage = "TODO: Put your gem's website or public repo URL here."
  spec.license = "MIT"
  spec.required_ruby_version = ">= 3.1.0"

  spec.metadata["allowed_push_host"] = "TODO: Set to your gem server 'https://example.com'"

  spec.metadata["homepage_uri"] = spec.homepage
  spec.metadata["source_code_uri"] = "TODO: Put your gem's public repo URL here."
  spec.metadata["changelog_uri"] = "TODO: Put your gem's CHANGELOG.md URL here."
  spec.authors     = ["Trevor John"]
  spec.email       = ""

  gemspec = File.basename(__FILE__)
  spec.files = IO.popen(%w[git ls-files -z], chdir: __dir__, err: IO::NULL) do |ls|
    ls.readlines("\x0", chomp: true).reject do |f|
      (f == gemspec) ||
        f.start_with?(*%w[bin/ test/ spec/ features/ .git appveyor Gemfile])
    end
  end
  spec.homepage    = "https://rubygems.org/gems/tigerbeetle"
  spec.license       = "Apache-2.0"
  spec.bindir = "exe"
  spec.executables = spec.files.grep(%r{\Aexe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.add_dependency "ffi", ">= 1.0"
end

