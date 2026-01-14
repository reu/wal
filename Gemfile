# frozen_string_literal: true

source "https://rubygems.org"

# Specify your gem's dependencies in wal.gemspec
gemspec

gem "irb"
gem "rake", "~> 13.0"
gem "debug"

group :test do
  gem "rails"
  gem "rspec", "~> 3.0"
  gem "testcontainers-postgres", require: "testcontainers/postgres"
end
