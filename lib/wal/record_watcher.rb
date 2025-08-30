module Wal
  # Watcher that process records at the end of a transaction, keeping only its final state.
  #
  # Example:
  #
  # ```ruby
  # class InventoryAvailabilityWatcher < Wal::RecordWatcher
  #   on_save Item, changed: %w[weight_unid_id] do |event|
  #     recalculate_inventory_availability(event.primary_key)
  #   end
  #
  #   on_save SalesOrder, changed: %w[status] do |event|
  #     next unless event.attributes_changes(:status).one? "filled"
  #
  #     OrderItem
  #       .where(sales_order_id: event.primary_key)
  #       .pluck(:item_id)
  #       .each { |item_id| recalculate_inventory_availability(item_id) }
  #   end
  #
  #   on_save OrderItem, changed: %w[item_id weight_unit weight_unid_id] do |event|
  #     if (old_item_id, new_item_id = event.attributes_changes("item_id"))
  #       recalculate_inventory_availability(old_item_id)
  #       recalculate_inventory_availability(new_item_id)
  #     else
  #       recalculate_inventory_availability(event.attribute(:item_id))
  #     end
  #   end
  #
  #   on_destroy OrderItem do |event|
  #     recalculate_inventory_availability(event.attribute(:item_id))
  #   end
  #
  #   def recalculate_inventory_availability(item_id)
  #     ...
  #   end
  # end
  # ```
  class RecordWatcher
    include Wal::Watcher

    def self.inherited(subclass)
      super
      @@change_callbacks = Hash.new { |hash, key| hash[key] = [] }
      @@delete_callbacks = Hash.new { |hash, key| hash[key] = [] }
    end

    def self.on_insert(table, &block)
      table = table.is_a?(String) ? table : table.table_name
      @@change_callbacks[table].push(only: [:create], block: block)
    end

    def self.on_update(table, changed: nil, &block)
      table = table.is_a?(String) ? table : table.table_name
      @@change_callbacks[table].push(only: [:update], changed: changed&.map(&:to_s), block: block)
    end

    def self.on_save(table, changed: nil, &block)
      table = table.is_a?(String) ? table : table.table_name
      @@change_callbacks[table].push(only: [:create, :update], changed: changed&.map(&:to_s), block: block)
    end

    def self.on_destroy(table, &block)
      table = table.is_a?(String) ? table : table.table_name
      @@delete_callbacks[table].push(block: block)
    end

    def self.change_callbacks
      @@change_callbacks
    end

    def self.delete_callbacks
      @@delete_callbacks
    end

    def on_record_changed(event)
      case event
      when InsertEvent
        @@change_callbacks[event.table]
          .filter { |callback| callback[:only].include? :create }
          .each { |callback| instance_exec(event, &callback[:block]) }

      when UpdateEvent
        @@change_callbacks[event.table]
          .filter { |callback| callback[:only].include? :update }
          .each do |callback|
            if (attributes = callback[:changed])
              instance_exec(event, &callback[:block]) unless (event.diff.keys & attributes).empty?
            else
              instance_exec(event, &callback[:block])
            end
          end

      when DeleteEvent
        @@delete_callbacks[event.table].each do |callback|
          instance_exec(event, &callback[:block])
        end
      end
    end

    def should_watch_table?(table)
      (@@change_callbacks.keys | @@delete_callbacks.keys).include? table
    end

    # `RecordWatcher` supports two processing strategies:
    #
    # `:memory`: Stores and aggregates records from a single transaction in memory. This has better performance but uses
    #  more memory, as at least one event for each record must be stored in memory until the end of a transaction
    #
    # `:temporary_table`: Offloads the record aggregation to a temporary table on the database. This is useful when you
    # are processing very large transactions that can't fit in memory. The tradeoff is obviously a worse performance.
    #
    # These strategies can be defined per transaction, and by default it will uses the memory one, and only fallback
    # to the temporary table if the transaction size is roughly 2 gigabytes or more.
    def aggregation_strategy(begin_transaction_event)
      if begin_transaction_event.estimated_size > 1024.pow(3) * 2
        :temporary_table
      else
        :memory
      end
    end

    def on_event(event)
      if event.is_a? BeginTransactionEvent
        @current_record_watcher = case (strategy = aggregation_strategy(event))
        when :memory
          MemoryRecordWatcher.new(self)
        when :temporary_table
          TemporaryTableRecordWatcher.new(self)
        else
          raise "Invalid aggregation strategy: #{strategy}"
        end
      end
      @current_record_watcher.on_event(event)
    end

    class MemoryRecordWatcher
      include Wal::Watcher
      include Wal::Watcher::SeparatedEvents

      def initialize(watcher)
        @watcher = watcher
      end

      def on_begin(event)
        @records = {}
      end

      def on_commit(_event)
        @records
          &.values
          &.lazy
          &.each { |event| @watcher.on_record_changed(event) if event }
      end

      def on_insert(event)
        if (id = event.primary_key)
          @records ||= {}
          @records[[event.table, id]] = event
        end
      end

      def on_update(event)
        if (id = event.primary_key)
          @records ||= {}
          @records[[event.table, id]] = case (existing_event = @records[[event.table, id]])
          when InsertEvent
            # A record inserted on this transaction is being updated, which means it should still reflect as a insert
            # event, we just change the information to reflect the most current data that was just updated.
            existing_event.with(new: event.new)

          when UpdateEvent
            # We are updating again a event that was already updated on this transaction.
            # Same as the insert, we keep the old data from the previous update and the new data from the new one.
            existing_event.with(new: event.new)

          else
            event
          end
        end
      end

      def on_delete(event)
        if (id = event.primary_key)
          @records ||= {}
          @records[[event.table, id]] = case (existing_event = @records[[event.table, id]])
          when InsertEvent
            # We are removing a record that was inserted on this transaction, we should not even report this change, as
            # this record never existed outside this transaction anyways.
            nil

          when UpdateEvent
            # Deleting a record that was previously updated by this transaction. Just store the previous data while
            # keeping the record as deleted.
            event.with(old: existing_event.old)

          else
            event
          end
        end
      end
    end

    class TemporaryTableRecordWatcher
      include Wal::Watcher
      include Wal::Watcher::SeparatedEvents

      # ActiveRecord base class used to persist the temporary table. Defaults to `ActiveRecord::Base`, but can be
      # changed if you want, for example, to use a different database for Wal processing.
      # Note that the class specified here must be a `abstract_class`.
      mattr_accessor :base_active_record_class

      def initialize(watcher, batch_size: 5_000)
        @watcher = watcher
        @batch_size = 5_000
      end

      def on_begin(event)
        @table = begin
          table_name = "temp_record_watcher_#{SecureRandom.alphanumeric(10).downcase}"

          base_class.connection.create_table(table_name, temporary: true) do |t|
            t.bigint :transaction_id, null: false
            t.bigint :lsn, null: false
            t.column :action, :string, null: false
            t.string :table_name, null: false
            t.bigint :primary_key
            t.jsonb :old, null: false, default: {}
            t.jsonb :new, null: false, default: {}
            t.jsonb :context, null: false, default: {}
          end

          unique_index = %i[table_name primary_key]

          base_class.connection.add_index table_name, unique_index, unique: true

          Class.new(base_class) do
            self.table_name = table_name

            # All this sh$#1t was necessary because AR schema cache doesn't work with temporary tables...
            insert_all_class = Class.new(::ActiveRecord::InsertAll) do
              unique_index_definition = ::ActiveRecord::ConnectionAdapters::IndexDefinition.new(
                table_name, "unique_index", true, unique_index
              )
              define_method(:find_unique_index_for) { |_| unique_index_definition }
            end

            define_singleton_method(:upsert) do |attributes, update_only: nil|
              insert_all_class
                .new(
                  none,
                  connection,
                  [attributes],
                  on_duplicate: :update,
                  unique_by: unique_index,
                  update_only:,
                  returning: nil,
                  record_timestamps: nil,
                )
                .execute
            end
          end
        end
      end

      def on_commit(_event)
        @table
          .in_batches(of: @batch_size)
          .each_record
          .lazy
          .filter_map { |persisted_event| deserialize(persisted_event) }
          .each { |event| @watcher.on_record_changed(event) }

        base_class.connection.drop_table @table.table_name
      end

      def on_insert(event)
        @table.upsert(serialize(event))
      end

      def on_update(event)
        @table.upsert(serialize(event), update_only: %w[new])
      end

      def on_delete(event)
        case @table.where(table_name: event.table, primary_key: event.primary_key).pluck(:action, :old).first
        in ["insert", _]
          @table.where(table_name: event.table, primary_key: event.primary_key).delete_all
        in ["update", old]
          @table.upsert(serialize(event).merge(old:))
        in ["delete", _]
          # We don't need to store another delete
        else
          @table.upsert(serialize(event))
        end
      end

      private

      def base_class
        self.class.base_active_record_class || ::ActiveRecord::Base
      end

      def serialize(event)
        serialized = {
          transaction_id: event.transaction_id,
          lsn: event.lsn,
          table_name: event.table,
          primary_key: event.primary_key,
          context: event.context,
        }
        case event
        when InsertEvent
          serialized.merge(action: "insert", new: event.new)
        when UpdateEvent
          serialized.merge(action: "update", old: event.old, new: event.new)
        when DeleteEvent
          serialized.merge(action: "delete", old: event.old)
        else
          serialized
        end
      end

      def deserialize(persisted_event)
        deserialized = {
          transaction_id: persisted_event.transaction_id,
          lsn: persisted_event.lsn,
          table: persisted_event.table_name,
          primary_key: persisted_event.primary_key,
          context: persisted_event.context,
        }
        case persisted_event.action
        when "insert"
          InsertEvent.new(**deserialized, new: persisted_event.new)
        when "update"
          UpdateEvent.new(**deserialized, old: persisted_event.old, new: persisted_event.new)
        when "delete"
          DeleteEvent.new(**deserialized, old: persisted_event.old)
        end
      end
    end
  end
end
