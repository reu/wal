require "pg"
require "pg/replication"
require "active_support"
require "active_record"
require_relative "wal/watcher"
require_relative "wal/noop_watcher"
require_relative "wal/logging_watcher"
require_relative "wal/record_watcher"
require_relative "wal/streaming_watcher"
require_relative "wal/replicator"
require_relative "wal/active_record_context_extension"
require_relative "wal/railtie"
require_relative "wal/runner"
require_relative "wal/version"

module Wal
  class << self
    attr_accessor :logger
    attr_accessor :fork_hooks

    def logger
      @logger ||= Logger.new($stdout, level: :info)
    end

    def fork_hooks
      @fork_hooks ||= {}
    end

    def before_fork(&block)
      fork_hooks[:before_fork] = block
    end

    def after_fork(&block)
      fork_hooks[:after_fork] = block
    end
  end

  def self.configure(&block)
    yield self
  end

  class BeginTransactionEvent < Data.define(:transaction_id, :lsn, :final_lsn, :timestamp)
    def estimated_size
      final_lsn - lsn
    end
  end

  class CommitTransactionEvent < Data.define(:transaction_id, :lsn, :context, :timestamp)
  end

  module TableName
    def full_table_name
      case schema
      in "public"
        table
      else
        "#{schema}.#{table}"
      end
    end
  end

  module ChangeEvent
    def diff
      {}
    end

    def changed_attribute?(attribute)
      diff.key? attribute.to_s
    end

    def attribute(attribute)
      if (changes = diff[attribute.to_s])
        changes[1]
      end
    end

    def attribute_changes(attribute)
      diff[attribute.to_s]
    end

    def attribute_was(attribute)
      if (changes = diff[attribute.to_s])
        changes[0]
      end
    end
  end

  class InsertEvent < Data.define(:transaction_id, :lsn, :context, :schema, :table, :primary_key, :new)
    include ::Wal::TableName
    include ::Wal::ChangeEvent

    def diff
      new.transform_values { |val| [nil, val] }
    end
  end

  class UpdateEvent < Data.define(:transaction_id, :lsn, :context, :schema, :table, :primary_key, :old, :new)
    include ::Wal::TableName
    include ::Wal::ChangeEvent

    def diff
      (old.keys | new.keys).reduce({}) do |diff, key|
        old[key] != new[key] ? diff.merge(key => [old[key], new[key]]) : diff
      end
    end
  end

  class DeleteEvent < Data.define(:transaction_id, :lsn, :context, :schema, :table, :primary_key, :old)
    include ::Wal::TableName
    include ::Wal::ChangeEvent

    def diff
      old.transform_values { |val| [val, nil] }
    end
  end
end
