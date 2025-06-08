# typed: strict

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
require_relative "wal/version"

module Wal
  Event = T.type_alias do
    T.any(
      BeginTransactionEvent,
      CommitTransactionEvent,
      InsertEvent,
      UpdateEvent,
      DeleteEvent,
    )
  end

  class BeginTransactionEvent < T::Struct
    extend T::Sig

    const :transaction_id, Integer
    const :lsn, Integer
    const :final_lsn, Integer
    const :timestamp, Time

    sig { returns(Integer) }
    def estimated_size
      final_lsn - lsn
    end
  end

  class CommitTransactionEvent < T::Struct
    const :transaction_id, Integer
    const :lsn, Integer
    const :context, T::Hash[String, T.untyped]
    const :timestamp, Time
  end

  module DiffedEvent
    extend T::Sig

    sig { returns(T::Hash[String, [T.untyped, T.untyped]]) }
    def diff
      {}
    end

    sig { params(attribute: T.any(Symbol, String)).returns(T::Boolean) }
    def changed_attribute?(attribute)
      diff.key? attribute.to_s
    end

    sig { params(attribute: T.any(Symbol, String)).returns(T.untyped) }
    def attribute(attribute)
      if (changes = diff[attribute.to_s])
        changes[1]
      end
    end

    sig { params(attribute: T.any(Symbol, String)).returns(T.nilable([T.untyped, T.untyped])) }
    def attribute_changes(attribute)
      diff[attribute.to_s]
    end

    sig { params(attribute: T.any(Symbol, String)).returns(T.untyped) }
    def attribute_was(attribute)
      if (changes = diff[attribute.to_s])
        changes[0]
      end
    end
  end

  class InsertEvent < T::Struct
    extend T::Sig
    include DiffedEvent

    const :transaction_id, Integer
    const :lsn, Integer
    const :context, T::Hash[String, T.untyped]
    const :table, String
    const :primary_key, T.untyped
    const :new, T::Hash[String, T.untyped]

    sig { returns(T::Hash[String, [T.untyped, T.untyped]]) }
    def diff
      new.transform_values { |val| [nil, val] }
    end
  end

  class UpdateEvent < T::Struct
    extend T::Sig
    include DiffedEvent

    const :transaction_id, Integer
    const :lsn, Integer
    const :context, T::Hash[String, T.untyped]
    const :table, String
    const :primary_key, T.untyped
    const :old, T::Hash[String, T.untyped]
    const :new, T::Hash[String, T.untyped]

    sig { returns(T::Hash[String, [T.untyped, T.untyped]]) }
    def diff
      (old.keys | new.keys).reduce({}) do |diff, key|
        old[key] != new[key] ? diff.merge(key => [old[key], new[key]]) : diff
      end
    end
  end

  class DeleteEvent < T::Struct
    extend T::Sig
    include DiffedEvent

    const :transaction_id, Integer
    const :lsn, Integer
    const :context, T::Hash[String, T.untyped]
    const :table, String
    const :primary_key, T.untyped
    const :old, T::Hash[String, T.untyped]

    sig { returns(T::Hash[String, [T.untyped, T.untyped]]) }
    def diff
      old.transform_values { |val| [val, nil] }
    end
  end
end
