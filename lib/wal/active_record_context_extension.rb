require "active_record/connection_adapters/postgresql_adapter"

module Wal
  module ActiveRecordContextExtension
    def set_wal_watcher_context(context, prefix: "")
      execute "SELECT pg_logical_emit_message(true, #{quote(prefix)}, #{quote(context.to_json)})" if transaction_open?
    end
  end
end

if defined? ActiveRecord::ConnectionAdapters::PostgreSQLAdapter
  class ActiveRecord::ConnectionAdapters::PostgreSQLAdapter
    include Wal::ActiveRecordContextExtension
  end
end
