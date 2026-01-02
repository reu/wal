# typed: strong
module Wal
  Event = T.type_alias { T.any(
      BeginTransactionEvent,
      CommitTransactionEvent,
      InsertEvent,
      UpdateEvent,
      DeleteEvent,
    ) }
  VERSION = "0.0.24"

  class << self
    sig { returns(T.class_of(Logger)) }
    attr_accessor :logger
  end

  sig { params(block: T.proc.params(config: T.class_of(Wal)).void).void }
  def self.configure(&block); end

  class BeginTransactionEvent < T::Struct
    prop :transaction_id, Integer, immutable: true
    prop :lsn, Integer, immutable: true
    prop :final_lsn, Integer, immutable: true
    prop :timestamp, Time, immutable: true

    extend T::Sig

    sig { returns(Integer) }
    def estimated_size; end
  end

  class CommitTransactionEvent < T::Struct
    prop :transaction_id, Integer, immutable: true
    prop :lsn, Integer, immutable: true
    prop :context, T::Hash[String, T.untyped], immutable: true
    prop :timestamp, Time, immutable: true

  end

  module TableName
    extend T::Sig

    sig { returns(String) }
    def full_table_name; end
  end

  module ChangeEvent
    extend T::Sig

    sig { returns(T::Hash[String, [T.untyped, T.untyped]]) }
    def diff; end

    sig { params(attribute: T.any(Symbol, String)).returns(T::Boolean) }
    def changed_attribute?(attribute); end

    sig { params(attribute: T.any(Symbol, String)).returns(T.untyped) }
    def attribute(attribute); end

    sig { params(attribute: T.any(Symbol, String)).returns(T.nilable([T.untyped, T.untyped])) }
    def attribute_changes(attribute); end

    sig { params(attribute: T.any(Symbol, String)).returns(T.untyped) }
    def attribute_was(attribute); end
  end

  class InsertEvent < T::Struct
    prop :transaction_id, Integer, immutable: true
    prop :lsn, Integer, immutable: true
    prop :context, T::Hash[String, T.untyped], immutable: true
    prop :schema, String, immutable: true
    prop :table, String, immutable: true
    prop :primary_key, T.untyped, immutable: true
    prop :new, T::Hash[String, T.untyped], immutable: true

    include ::Wal::TableName
    include ::Wal::ChangeEvent
    extend T::Sig

    sig { returns(T::Hash[String, [T.untyped, T.untyped]]) }
    def diff; end
  end

  class UpdateEvent < T::Struct
    prop :transaction_id, Integer, immutable: true
    prop :lsn, Integer, immutable: true
    prop :context, T::Hash[String, T.untyped], immutable: true
    prop :schema, String, immutable: true
    prop :table, String, immutable: true
    prop :primary_key, T.untyped, immutable: true
    prop :old, T::Hash[String, T.untyped], immutable: true
    prop :new, T::Hash[String, T.untyped], immutable: true

    include ::Wal::TableName
    include ::Wal::ChangeEvent
    extend T::Sig

    sig { returns(T::Hash[String, [T.untyped, T.untyped]]) }
    def diff; end
  end

  class DeleteEvent < T::Struct
    prop :transaction_id, Integer, immutable: true
    prop :lsn, Integer, immutable: true
    prop :context, T::Hash[String, T.untyped], immutable: true
    prop :schema, String, immutable: true
    prop :table, String, immutable: true
    prop :primary_key, T.untyped, immutable: true
    prop :old, T::Hash[String, T.untyped], immutable: true

    include ::Wal::TableName
    include ::Wal::ChangeEvent
    extend T::Sig

    sig { returns(T::Hash[String, [T.untyped, T.untyped]]) }
    def diff; end
  end

  module ActiveRecordContextExtension
    sig { params(context: T.untyped, prefix: T.untyped).returns(T.untyped) }
    def set_wal_watcher_context(context, prefix: ""); end
  end

  class NoopWatcher
    include Wal::Watcher
    extend T::Sig

    sig { override.params(event: Wal::Event).void }
    def on_event(event); end
  end

  class RecordWatcher
    abstract!

    include Wal::Watcher
    extend T::Sig
    extend T::Helpers
    RecordEvent = T.type_alias { T.any(Wal::InsertEvent, Wal::UpdateEvent, Wal::DeleteEvent) }

    sig { params(subclass: T.untyped).returns(T.untyped) }
    def self.inherited(subclass); end

    sig { params(table: T.any(String, T.class_of(::ActiveRecord::Base)), block: T.proc.bind(T.attached_class).params(event: Wal::InsertEvent).void).void }
    def self.on_insert(table, &block); end

    sig { params(table: T.any(String, T.class_of(::ActiveRecord::Base)), changed: T.nilable(T::Array[T.any(String, Symbol)]), block: T.proc.bind(T.attached_class).params(event: Wal::UpdateEvent).void).void }
    def self.on_update(table, changed: nil, &block); end

    sig { params(table: T.any(String, T.class_of(::ActiveRecord::Base)), changed: T.nilable(T::Array[T.any(String, Symbol)]), block: T.proc.bind(T.attached_class).params(event: T.any(Wal::InsertEvent, Wal::UpdateEvent)).void).void }
    def self.on_save(table, changed: nil, &block); end

    sig { params(table: T.any(String, T.class_of(::ActiveRecord::Base)), block: T.proc.bind(T.attached_class).params(event: Wal::DeleteEvent).void).void }
    def self.on_delete(table, &block); end

    sig { params(event: RecordEvent).void }
    def on_record_changed(event); end

    sig { params(table: String).returns(T::Boolean) }
    def should_watch_table?(table); end

    sig { params(event: Wal::BeginTransactionEvent).returns(Symbol) }
    def aggregation_strategy(event); end

    sig { override.params(event: Wal::Event).void }
    def on_event(event); end
  end

  class Replicator
    include PG::Replication::Protocol
    extend T::Sig

    sig { params(replication_slot: String, use_temporary_slot: T::Boolean, db_config: T::Hash[Symbol, T.untyped]).void }
    def initialize(replication_slot:, use_temporary_slot: false, db_config: ActiveRecord::Base.configurations.configs_for(name: "primary").configuration_hash); end

    sig { params(watcher: Wal::Watcher, publications: T::Array[String]).void }
    def replicate_forever(watcher, publications:); end

    sig { params(watcher: Wal::Watcher, publications: T::Array[String]).returns(T::Enumerator::Lazy[Wal::Event]) }
    def replicate(watcher, publications:); end
  end

  class StreamingWatcher
    abstract!

    include Wal::Watcher
    extend T::Sig
    extend T::Helpers

    sig { abstract.params(events: T::Enumerator[Wal::Event]).void }
    def on_transaction_events(events); end

    sig { params(event: Wal::BeginTransactionEvent).returns(Integer) }
    def queue_size(event); end

    sig { override.params(event: Wal::Event).void }
    def on_event(event); end
  end

  module Watcher
    abstract!

    extend T::Sig
    extend T::Helpers

    sig { abstract.params(event: Wal::Event).void }
    def on_event(event); end

    sig { params(table: String).returns(T::Boolean) }
    def should_watch_table?(table); end

    sig { params(prefix: String).returns(T::Boolean) }
    def valid_context_prefix?(prefix); end

    module SeparatedEvents
      extend T::Sig

      sig { params(event: Wal::Event).void }
      def on_event(event); end

      sig { params(event: Wal::BeginTransactionEvent).void }
      def on_begin(event); end

      sig { params(event: Wal::InsertEvent).void }
      def on_insert(event); end

      sig { params(event: Wal::UpdateEvent).void }
      def on_update(event); end

      sig { params(event: Wal::DeleteEvent).void }
      def on_delete(event); end

      sig { params(event: Wal::CommitTransactionEvent).void }
      def on_commit(event); end
    end
  end
end
