# typed: strict

module Wal
  # A watcher that does nothing. Just for performance testing in general. Useful in testing aswell.
  class NoopWatcher
    extend T::Sig
    include Wal::Watcher

    sig { override.params(event: Event).void }
    def on_event(event); end
  end
end
