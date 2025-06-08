# typed: true

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
  #   sig { override.params(events: T::Enumerator[Event]).void }
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
    extend T::Sig
    extend T::Helpers
    include Wal::Watcher
    abstract!

    sig { abstract.params(events: T::Enumerator[Event]).void }
    def on_transaction_events(events); end

    sig { params(event: BeginTransactionEvent).returns(Integer) }
    def queue_size(event)
      5_000
    end

    sig { override.params(event: Event).void }
    def on_event(event)
      case event
      when BeginTransactionEvent
        @queue = SizedQueue.new(queue_size(event))

        event_stream = Enumerator.new do |y|
          while (item = @queue.pop)
            case item
            when CommitTransactionEvent
              y << item
              break
            else
              y << item
            end
          end
        end
        @worker = Thread.new { on_transaction_events(event_stream) }

        @queue << event

      when CommitTransactionEvent
        @queue << event
        @worker.join

        # We are cleaning this up to hint to Ruby GC that this can be freed before the next begin transaction arrives
        @queue.clear
        @queue = nil

      else
        @queue << event
      end
    end
  end
end
