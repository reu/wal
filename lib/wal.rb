require "pg"
require "pg/replication"
require "active_support"
require "active_record"
require_relative "wal/watcher"
require_relative "wal/noop_watcher"
require_relative "wal/record_watcher"
require_relative "wal/streaming_watcher"
require_relative "wal/replicator"
require_relative "wal/active_record_context_extension"
require_relative "wal/railtie"
require_relative "wal/version"

module Wal
  class << self
    attr_accessor :logger
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

  class InsertEvent < Data.define(:transaction_id, :lsn, :context, :table, :primary_key, :new)
    include ::Wal::ChangeEvent

    def diff
      new.transform_values { |val| [nil, val] }
    end
  end

  class UpdateEvent < Data.define(:transaction_id, :lsn, :context, :table, :primary_key, :old, :new)
    include ::Wal::ChangeEvent

    def diff
      (old.keys | new.keys).reduce({}) do |diff, key|
        old[key] != new[key] ? diff.merge(key => [old[key], new[key]]) : diff
      end
    end
  end

  class DeleteEvent < Data.define(:transaction_id, :lsn, :context, :table, :primary_key, :old)
    include ::Wal::ChangeEvent

    def diff
      old.transform_values { |val| [val, nil] }
    end
  end
end
