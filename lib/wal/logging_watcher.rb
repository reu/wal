module Wal
  class LoggingWatcher
    include Wal::Watcher

    def initialize(slot, watcher)
      @slot = slot
      @watcher = watcher
      @actions = Set.new
      @tables = Set.new
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
        @start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        @count = 0
        Wal.logger&.debug("[#{@slot}] Begin transaction=#{event.transaction_id} size=#{event.estimated_size}")
      when Wal::CommitTransactionEvent
        elapsed = ((Process.clock_gettime(Process::CLOCK_MONOTONIC) - @start) * 1000).round(3)
        message = "[#{@slot}] Commit transaction=#{event.transaction_id} elapsed=#{elapsed} events=#{@count}"
        message << " actions=#{@actions.sort.join(",")}" unless @actions.empty?
        message << " tables=#{@tables.sort.join(",")}" unless @tables.empty?
        if @count > 1
          Wal.logger&.info(message)
        else
          Wal.logger&.debug(message)
        end
        @actions.clear
        @tables.clear
      when Wal::InsertEvent
        Wal.logger&.debug("[#{@slot}] Insert transaction=#{event.transaction_id} table=#{event.table} primary_key=#{event.primary_key}")
        @count += 1
        @actions << :insert
        @tables << event.table
      when Wal::UpdateEvent
        if Wal.logger&.level >= Logger::DEBUG
          message = "[#{@slot}] Update transaction=#{event.transaction_id} table=#{event.table} primary_key=#{event.primary_key}"
          message << " changed=#{event.diff.keys.join(",")}" if event.diff && !event.diff.empty?
          Wal.logger&.debug(message)
        end
        @count += 1
        @actions << :update
        @tables << event.table
      when Wal::DeleteEvent
        Wal.logger&.debug("[#{@slot}] Delete transaction=#{event.transaction_id} table=#{event.table} primary_key=#{event.primary_key}")
        @count += 1
        @actions << :delete
        @tables << event.table
      else
        @count += 1
      end
      @watcher.on_event(event)
    end
  end
end
