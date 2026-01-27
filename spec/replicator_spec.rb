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

    it "supports multiple schemas" do
      class AlternateRecord < ActiveRecord::Base
        self.table_name = "alternate.records"
      end

      watcher = BufferWatcher.new
      replication = create_testing_wal_replication(watcher, db_config: @pg_config)

      ActiveRecord::Base.transaction do
        AlternateRecord.create!(name: "OriginalName")
      end

      replicate_single_transaction(replication)

      insert_event = watcher.received_events[1]

      assert_equal "alternate", insert_event.schema
      assert_equal "records", insert_event.table
    end

    it "supports composite primary keys" do
      watcher = BufferWatcher.new
      replication = create_testing_wal_replication(watcher, db_config: @pg_config)

      ActiveRecord::Base.transaction do
        ActiveRecord::Base.connection.execute(
          "INSERT INTO order_items (order_id, product_id, quantity) VALUES (1, 100, 5)"
        )
      end

      replicate_single_transaction(replication)

      insert_event = watcher.received_events[1]

      assert_instance_of Wal::InsertEvent, insert_event
      assert_equal "order_items", insert_event.table

      # Verify composite primary key is an array
      assert_instance_of Array, insert_event.primary_key
      assert_equal [1, 100], insert_event.primary_key
    end

    it "supports composite primary key updates and deletes" do
      watcher = BufferWatcher.new
      replication = create_testing_wal_replication(watcher, db_config: @pg_config)

      ActiveRecord::Base.transaction do
        ActiveRecord::Base.connection.execute(
          "INSERT INTO order_items (order_id, product_id, quantity) VALUES (2, 200, 10)"
        )
        ActiveRecord::Base.connection.execute(
          "UPDATE order_items SET quantity = 20 WHERE order_id = 2 AND product_id = 200"
        )
        ActiveRecord::Base.connection.execute(
          "DELETE FROM order_items WHERE order_id = 2 AND product_id = 200"
        )
      end

      replicate_single_transaction(replication)

      insert_event = watcher.received_events[1]
      update_event = watcher.received_events[2]
      delete_event = watcher.received_events[3]

      # All events should have the same composite primary key
      assert_equal [2, 200], insert_event.primary_key
      assert_equal [2, 200], update_event.primary_key
      assert_equal [2, 200], delete_event.primary_key

      # Verify update event data
      assert_equal 10, update_event.old["quantity"]
      assert_equal 20, update_event.new["quantity"]
    end

    it "supports string primary keys" do
      watcher = BufferWatcher.new
      replication = create_testing_wal_replication(watcher, db_config: @pg_config)

      uuid = SecureRandom.uuid

      ActiveRecord::Base.transaction do
        ActiveRecord::Base.connection.execute(
          "INSERT INTO uuid_records (id, name) VALUES ('#{uuid}', 'TestRecord')"
        )
      end

      replicate_single_transaction(replication)

      insert_event = watcher.received_events[1]

      assert_instance_of Wal::InsertEvent, insert_event
      assert_equal "uuid_records", insert_event.table

      # Verify string primary key
      assert_instance_of String, insert_event.primary_key
      assert_equal uuid, insert_event.primary_key
    end

    it "discovers primary key from PostgreSQL catalogs" do
      watcher = BufferWatcher.new
      replication = create_testing_wal_replication(watcher, db_config: @pg_config)

      # Insert into the regular records table with auto-generated id
      record_id = ActiveRecord::Base.transaction do
        record = Record.create(name: "TestDiscovery")
        record.id
      end

      replicate_single_transaction(replication)

      insert_event = watcher.received_events[1]

      # The primary key should be discovered from pg_constraint, not hardcoded
      assert_instance_of Integer, insert_event.primary_key
      assert_equal record_id, insert_event.primary_key
    end

    it "handles unchanged toasted values in update and delete events" do
      watcher = BufferWatcher.new
      replication = create_testing_wal_replication(watcher, db_config: @pg_config)

      # Create large text values that will be TOASTed (> 2KB)
      large_content = "x" * 10_000
      updated_large_content = "y" * 10_000

      record_id = ActiveRecord::Base.transaction do
        record = ToastableRecord.create!(name: "Original", large_content: large_content)
        # Update only the name, leaving large_content unchanged
        # PostgreSQL will send a "toast unchanged" marker for large_content
        record.update!(name: "Updated")
        # Update the large_content to verify changed toasted values are reported correctly
        record.update!(large_content: updated_large_content)
        record.destroy!
        record.id
      end

      replicate_single_transaction(replication)

      insert_event = watcher.received_events[1]
      update_event_name_only = watcher.received_events[2]
      update_event_content = watcher.received_events[3]
      delete_event = watcher.received_events[4]

      assert_instance_of Wal::InsertEvent, insert_event
      assert_instance_of Wal::UpdateEvent, update_event_name_only
      assert_instance_of Wal::UpdateEvent, update_event_content
      assert_instance_of Wal::DeleteEvent, delete_event

      # Verify the insert event has the large content
      assert_equal record_id, insert_event.primary_key
      assert_equal "Original", insert_event.new["name"]
      assert_equal large_content, insert_event.new["large_content"]

      # Verify the first update event properly handles the unchanged toasted value
      assert_equal record_id, update_event_name_only.primary_key
      assert_equal "Original", update_event_name_only.old["name"]
      assert_equal "Updated", update_event_name_only.new["name"]
      # The key assertion: unchanged toasted values should be present in old data
      # Without proper TOAST handling, this would be nil
      assert_equal large_content, update_event_name_only.old["large_content"]
      assert_equal large_content, update_event_name_only.new["large_content"]

      # Verify the second update event reports the changed toasted value correctly
      assert_equal record_id, update_event_content.primary_key
      assert_equal "Updated", update_event_content.old["name"]
      assert_equal "Updated", update_event_content.new["name"]
      assert_equal large_content, update_event_content.old["large_content"]
      assert_equal updated_large_content, update_event_content.new["large_content"]

      # Verify the delete event has the final toasted value
      assert_equal record_id, delete_event.primary_key
      assert_equal "Updated", delete_event.old["name"]
      assert_equal updated_large_content, delete_event.old["large_content"]
    end
  end
end
