module Wal
  # A watcher that does nothing. Just for performance testing in general. Useful in testing aswell.
  class NoopWatcher
    include Wal::Watcher

    def on_event(event); end
  end
end
