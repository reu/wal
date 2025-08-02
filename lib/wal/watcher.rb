module Wal
  # Watcher is the core API used to hook into Postgres WAL log.
  # The only required method on the API is the `on_event`, which will receive a WAL entry of the following events:
  # - Transaction started: `Wal::BeginTransactionEvent`
  # - Row inserted: `Wal::InsertEvent`
  # - Row updated: `Wal::UpdateEvent`
  # - Row deleted: `Wal::DeleteEvent`
  # - Transaction committed: `Wal::CommitTransactionEvent`
  #
  # The `on_event` method will be called without any buffering, so it is up to implementators to aggregate them if
  # desired. In practice, it is rarelly useful to implement this module directly for application level business logic,
  # and instead it is more recomended using more specific ones, such as the `RecordWatcher` and `StreamingWalWatcher`.
  module Watcher
    def on_event(event); end

    # Allows dropping the processing of any table
    def should_watch_table?(table)
      true
    end

    # Check if the given context prefix should be allowed for this watcher
    def valid_context_prefix?(prefix)
      true
    end

    # Include this module if you prefer to work with each event having its own method.
    # This might be useful when you always want to process each type of event in a different way.
    #
    # Example:
    #
    # Watcher that calculates how much time passed between the begin and commit of a WAL transaction.
    #
    # ```ruby
    # class MeasureTransactionTimeWatcher
    #   include Wal::Watcher
    #   include Wal::Watcher::SeparatedEvents
    #
    #   def on_begin(event)
    #     @start_time = Time.current
    #   end
    #
    #   def on_commit(event)
    #     puts "Transaction processing time: #{Time.current - @start_time}"
    #   end
    # end
    # ```
    module SeparatedEvents
      def on_event(event)
        case event
        when BeginTransactionEvent
          on_begin(event)
        when CommitTransactionEvent
          on_commit(event)
        when InsertEvent
          on_insert(event)
        when UpdateEvent
          on_update(event)
        when DeleteEvent
          on_delete(event)
        end
      end

      def on_begin(event); end
      def on_insert(event); end
      def on_update(event); end
      def on_delete(event); end
      def on_commit(event); end
    end
  end
end
