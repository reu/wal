# frozen_string_literal: true

module Wal
  module Migration
    class PublicationDefinition
      def initialize(name)
        @name = name
        @tables = {}
      end

      def table(name, columns: [])
        @tables[name] ||= [].to_set
        @tables[name].merge(columns.map(&:to_s))
      end

      def tables
        @tables.keys
      end

      def to_sql
        tables_sql = @tables.reduce([]) do |sql, (table, columns)|
          table_sql = "TABLE #{table}"
          table_sql << " (#{columns.join(", ")})" unless columns.empty?
          sql << table_sql
        end
        "ALTER PUBLICATION #{@name} SET #{tables_sql.join(", ")}"
      end
    end

    def define_publication(name)
      execute <<~SQL
        DO $$
        BEGIN
        CASE WHEN NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = '#{name}') THEN
          CREATE PUBLICATION #{name};
        ELSE NULL;
        END CASE;
        END;
        $$;
      SQL

      definition = PublicationDefinition.new(name)
      yield definition

      execute definition.to_sql
    end
  end
end
