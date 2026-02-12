module Wal
  # A watcher that streams all the events of each WAL transaction on a separate thread.
  #
  # Useful to improve the throughput, as it will allow you to process events while fetching for more in parallel.
  #
  # Example:
  #
  # Watcher that persists all delete events as it arrives using a single database transaction, and without waiting
  # for the full WAL log transaction to be finished.
  #
  # ```ruby
  # class RegisterDeletesWalWatcher < Wal::StreamingWalWatcher
  #   def on_transaction_events(events)
  #     DeletedApplicationRecord.transaction do
  #       events
  #         .lazy
  #         .filter { |event| event.is_a? DeleteEvent }
  #         .each { |event| DeletedApplicationRecord.create_from_event(event) }
  #     end
  #   end
  # end
  # ```
  class StreamingWatcher
    include Wal::Watcher

    def on_transaction_events(events); end

    def queue_size(event)
      5_000
    end

    def on_event(event)
      case event
      when BeginTransactionEvent
        @event_queue = SizedQueue.new(queue_size(event))
        event_stream = Enumerator.new do |y|
          while (item = @event_queue.pop)
            y << item
            break if item.is_a?(CommitTransactionEvent)
          end
        end
        ensure_worker
        @transaction_queue << event_stream
        @event_queue << event

      when CommitTransactionEvent
        @event_queue << event
        result = @completion_queue.pop
        @event_queue.clear
        raise result if result.is_a? Exception

      else
        @event_queue << event
      end
    end

    private

    def ensure_worker
      return if @worker&.alive?

      @transaction_queue = Queue.new
      @completion_queue = Queue.new

      @worker = Thread.new do
        while (event_stream = @transaction_queue.pop)
          begin
            on_transaction_events(event_stream)
            @completion_queue << :done
          rescue Exception => e
            @completion_queue << e
          end
        end
      end
    end
  end
end
