# frozen_string_literal: true

require "rails/railtie"

module Wal
  class Railtie < Rails::Railtie
    generators do
      require_relative "generators/migration"
    end

    config.before_configuration do
      require_relative "migration"
      ActiveRecord::Migration.include Wal::Migration
      ActiveRecord::Schema.include Wal::Migration
    end

    config.after_initialize do
      # "Borrowed" from Sidekiq
      # https://github.com/sidekiq/sidekiq/blob/61e27d20b1ed62f203eee6ae2b549f2e53db14c9/lib/sidekiq/rails.rb#L38-L58
      unless Rails.logger == Wal.logger || ActiveSupport::Logger.logger_outputs_to?(Rails.logger, $stdout)
        if Rails.logger.respond_to?(:broadcast_to)
          Rails.logger.broadcast_to(Wal.logger)
        elsif ActiveSupport::Logger.respond_to?(:broadcast)
          Rails.logger.extend(ActiveSupport::Logger.broadcast(Wal.logger))
        else
          Rails.logger = ActiveSupport::BroadcastLogger.new(Rails.logger, Wal.logger)
        end
      end
    end
  end
end
