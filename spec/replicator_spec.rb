require "spec_helper"

RSpec.describe Wal::Replicator do
  class BufferWatcher
    include Wal::Watcher

    attr_reader :received_events
    attr_reader :context_prefixes

    def initialize(context_prefixes: [])
      @received_events = []
      @context_prefixes = context_prefixes
    end

    def clear!
      @received_events = []
    end

    def on_event(event)
      @received_events << event
    end

    def valid_context_prefix?(prefix)
      context_prefixes.include? prefix
    end
  end

  describe "#replicate" do
    it "callbacks the watcher with the proper events" do
      watcher = BufferWatcher.new
      replication = create_testing_wal_replication(watcher, db_config: @pg_config)

      record_id = ActiveRecord::Base.transaction do
        record = Record.create(name: "OriginalName")
        record.name = "UpdatedName"
        record.save!
        record.destroy!
        record.id
      end

      replicate_single_transaction(replication)

      begin_transaction_event = watcher.received_events[0]
      insert_event = watcher.received_events[1]
      update_event = watcher.received_events[2]
      delete_event = watcher.received_events[3]
      commit_transaction_event = watcher.received_events[4]

      assert_instance_of Wal::BeginTransactionEvent, begin_transaction_event
      assert_instance_of Wal::InsertEvent, insert_event
      assert_instance_of Wal::UpdateEvent, update_event
      assert_instance_of Wal::DeleteEvent, delete_event
      assert_instance_of Wal::CommitTransactionEvent, commit_transaction_event

      transaction_id = begin_transaction_event.transaction_id

      assert_equal transaction_id, insert_event.transaction_id
      assert_equal record_id, insert_event.primary_key
      assert_equal Record.table_name, insert_event.table
      assert_equal "OriginalName", insert_event.new["name"]

      assert_equal transaction_id, update_event.transaction_id
      assert_equal record_id, update_event.primary_key
      assert_equal Record.table_name, insert_event.table
      assert_equal "OriginalName", update_event.old["name"]
      assert_equal "UpdatedName", update_event.new["name"]

      assert_equal transaction_id, delete_event.transaction_id
      assert_equal record_id, delete_event.primary_key
      assert_equal Record.table_name, insert_event.table
      assert_equal "UpdatedName", delete_event.old["name"]

      assert_equal transaction_id, commit_transaction_event.transaction_id
    end

    it "supports parsing context from events" do
      watcher = BufferWatcher.new(context_prefixes: ["test_context"])
      context1 = { name: "context1", list: [1, 2, 3] }.as_json
      context2 = { name: "context2", list: [1, 2, 3] }.as_json

      replication = create_testing_wal_replication(watcher, db_config: @pg_config)

      ActiveRecord::Base.transaction do
        # Setting the context for the next statements
        ActiveRecord::Base.connection.set_wal_watcher_context(context1, prefix: "test_context")
        record = Record.create(name: "OriginalName")
        record.name = "UpdatedName"
        record.save!

        # Change the context in the middle of the transaction
        ActiveRecord::Base.connection.set_wal_watcher_context(context2, prefix: "test_context")
        record.destroy!
      end

      replicate_single_transaction(replication)

      insert_event = watcher.received_events[1]
      update_event = watcher.received_events[2]
      delete_event = watcher.received_events[3]

      assert_equal context1, insert_event.context
      assert_equal context1, update_event.context
      assert_equal context2, delete_event.context
    end
  end
end
