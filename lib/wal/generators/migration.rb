# frozen_string_literal: true

require "rails/generators"
require "rails/generators/active_record"

module Wal
  module Generators
    class Migration < Rails::Generators::Base
      include Rails::Generators::Migration

      source_root File.expand_path("templates", __dir__)

      argument :watcher, type: :string, required: true, banner: "WATCHER_CLASS", desc: "Wal Watcher class name"
      class_option :columns, type: :boolean, required: false, default: false

      def self.next_migration_number(dir)
        ::ActiveRecord::Generators::Base.next_migration_number(dir)
      end

      def create_migration_file
        migration_template("migration.rb.erb", "db/migrate/#{migration_class_name.underscore}.rb")
      end

      private

      def activerecord_migration_class
        if ActiveRecord::Migration.respond_to? :current_version
          "ActiveRecord::Migration[#{ActiveRecord::Migration.current_version}]"
        else
          "ActiveRecord::Migration"
        end
      end

      def publication_name
        "#{class_name.gsub("Watcher", "").underscore}_publication"
      end

      def class_name
        watcher.camelize
      end

      def migration_class_name
        "Set#{class_name}Publication"
      end

      def publication_tables
        if (watcher = class_name.constantize) && watcher < Wal::RecordWatcher
          tables = watcher
            .change_callbacks
            .map { |table, configs| [table.to_s, configs.flat_map { |config| config[:changed] || [] }.uniq] }
            .to_h

          watcher
            .delete_callbacks
            .keys
            .reduce(tables) { |tables, table| tables.reverse_merge(table => []) }
            .transform_values { |cols| options[:columns] ? cols : [] }
        else
          []
        end
      end
    end
  end
end
