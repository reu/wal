# frozen_string_literal: true

require "rails/railtie"

module Waldit
  class Railtie < Rails::Railtie
    generators do
      require_relative "generators/migration"
    end

    config.before_configuration do
      require_relative "migration"
      ActiveRecord::Migration.include Wal::Migration
      ActiveRecord::Schema.include Wal::Migration
    end
  end
end
