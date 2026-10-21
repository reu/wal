require "spec_helper"

RSpec.describe Wal::LoggingWatcher do
  class RecordingWatcher
    include Wal::Watcher

    def initialize(calls, fail_on: nil, commit_delay: 0)
      @calls = calls
      @fail_on = fail_on
      @commit_delay = commit_delay
    end

    def on_event(event)
      raise "boom" if event.is_a?(@fail_on) if @fail_on
      sleep @commit_delay if event.is_a?(Wal::CommitTransactionEvent)
      @calls << [:watcher, event.class]
    end
  end

  let(:calls) { [] }
  let(:log) { StringIO.new }

  before do
    @previous_logger = Wal.logger
    Wal.logger = Logger.new(log, level: :info)
    Wal.on_transaction_start { |event, slot, _watcher| calls << [:start, event.class, slot] }
    Wal.on_transaction_finish { |event, slot, _watcher| calls << [:finish, event&.class, slot] }
  end

  after do
    Wal.logger = @previous_logger
    Wal.hooks.delete(:on_transaction_start)
    Wal.hooks.delete(:on_transaction_finish)
  end

  def transaction(id)
    [
      Wal::BeginTransactionEvent.new(transaction_id: id, lsn: 100, final_lsn: 200, timestamp: Time.now),
      Wal::InsertEvent.new(transaction_id: id, lsn: 110, context: {}, schema: "public", table: "records", primary_key: 1, new: {}),
      Wal::InsertEvent.new(transaction_id: id, lsn: 120, context: {}, schema: "public", table: "records", primary_key: 2, new: {}),
      Wal::CommitTransactionEvent.new(transaction_id: id, lsn: 200, context: {}, timestamp: Time.now),
    ]
  end

  it "wraps the whole transaction processing between the start and finish hooks" do
    watcher = Wal::LoggingWatcher.new("slot", RecordingWatcher.new(calls))
    transaction(1).each { |event| watcher.on_event(event) }

    assert_equal [
      [:start, Wal::BeginTransactionEvent, "slot"],
      [:watcher, Wal::BeginTransactionEvent],
      [:watcher, Wal::InsertEvent],
      [:watcher, Wal::InsertEvent],
      [:watcher, Wal::CommitTransactionEvent],
      [:finish, Wal::CommitTransactionEvent, "slot"],
    ], calls
  end

  it "logs the elapsed time including the watcher commit processing" do
    watcher = Wal::LoggingWatcher.new("slot", RecordingWatcher.new(calls, commit_delay: 0.05))
    transaction(1).each { |event| watcher.on_event(event) }

    elapsed = log.string[/elapsed=([\d.]+)/, 1].to_f
    assert_operator elapsed, :>=, 50
    assert_includes log.string, "events=2 actions=insert tables=records"
  end

  it "still calls the finish hook when the watcher raises mid-transaction" do
    watcher = Wal::LoggingWatcher.new("slot", RecordingWatcher.new(calls, fail_on: Wal::InsertEvent))
    begin_event, insert_event, = transaction(1)

    watcher.on_event(begin_event)
    assert_raises(RuntimeError) { watcher.on_event(insert_event) }

    assert_equal [:finish, nil, "slot"], calls.last
    assert_equal 1, calls.count { |call| call.first == :finish }
  end

  it "pairs the hooks again on the next transaction after an aborted one" do
    failing = RecordingWatcher.new(calls, fail_on: Wal::InsertEvent)
    watcher = Wal::LoggingWatcher.new("slot", failing)
    begin_event, insert_event, = transaction(1)
    watcher.on_event(begin_event)
    assert_raises(RuntimeError) { watcher.on_event(insert_event) }
    calls.clear

    failing.instance_variable_set(:@fail_on, nil)
    transaction(2).each { |event| watcher.on_event(event) }

    assert_equal [:start, Wal::BeginTransactionEvent, "slot"], calls.first
    assert_equal [:finish, Wal::CommitTransactionEvent, "slot"], calls.last
    assert_includes log.string, "events=2 actions=insert tables=records"
  end
end
