# frozen_string_literal: true

require "bundler/gem_tasks"

task(:test) { sh "bundle exec rspec" }

task default: %i[build]

task("sig/wal.rbi") { sh "bundle exec parlour" }
task("rbi/wal.rbs") { sh "rbs prototype rbi rbi/wal.rbi > sig/wal.rbs" }

Rake::Task["build"].enhance(["sig/wal.rbi", "rbi/wal.rbs"])
