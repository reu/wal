module Wal
  class LoggingWatcher
    include Wal::Watcher

    def initialize(slot, watcher)
      @slot = slot
      @watcher = watcher
    end

    def should_watch_table?(table)
      @watcher.should_watch_table? table
    end

    def valid_context_prefix?(prefix)
      @watcher.valid_context_prefix? prefix
    end

    def on_event(event)
      case event
      when Wal::BeginTransactionEvent
        @start = Time.now
        @count = 0
        if event.estimated_size > 0
          Wal.logger&.debug("[#{@slot}] Begin transaction=#{event.transaction_id} size=#{event.estimated_size}")
        end
      when Wal::CommitTransactionEvent
        if @count > 0
          elapsed = ((Time.now - @start) * 1000.0).round(1)
          Wal.logger&.info("[#{@slot}] Commit transaction=#{event.transaction_id} elapsed=#{elapsed} events=#{@count}")
        end
      when Wal::InsertEvent
        Wal.logger&.debug("[#{@slot}] Insert transaction=#{event.transaction_id} table=#{event.table} primary_key=#{event.primary_key}")
        @count += 1
      when Wal::UpdateEvent
        Wal.logger&.debug("[#{@slot}] Update transaction=#{event.transaction_id} table=#{event.table} primary_key=#{event.primary_key}")
        @count += 1
      when Wal::DeleteEvent
        Wal.logger&.debug("[#{@slot}] Delete transaction=#{event.transaction_id} table=#{event.table} primary_key=#{event.primary_key}")
        @count += 1
      else
        @count += 1
      end
      @watcher.on_event(event)
    end
  end
end
