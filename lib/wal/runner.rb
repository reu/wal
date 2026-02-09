module Wal
  class Runner
    class Worker
      attr_reader :name, :slot_configs, :db_config

      def initialize(name:, slot_configs:, db_config:)
        @name = name
        @slot_configs = slot_configs
        @db_config = db_config
        @threads = []
      end

      def run
        @threads = slot_configs.map { |slot, config| start_worker(slot, config) }
        setup_signal_handlers
        @threads.each(&:join)
      end

      private

      def start_worker(slot, config)
        watcher = config["watcher"].constantize.new
        temporary = config["temporary"] || false
        publications = config["publications"] || []
        replicator_class = config["replicator"].presence&.constantize || Wal::Replicator
        replicator_params = config["replicator_params"]&.transform_keys(&:to_sym) || {}
        auto_restart = config["auto_restart"].nil? || config["auto_restart"]
        max_retries = config["retries"]&.to_i || (2**32 - 1)
        backoff = config["retry_backoff"]&.to_f || 1
        backoff_exponent = config["retry_backoff_expoent"]&.to_f

        Thread.new(slot, watcher, temporary, publications) do |replication_slot, watcher, use_temporary_slot, publications|
          retries = 0
          replication_slot = "#{replication_slot}_#{SecureRandom.alphanumeric(4)}" if use_temporary_slot
          puts "Watcher started for #{replication_slot} slot (#{publications.join(", ")})"

          begin
            replicator_class
              .new(db_config:, **replicator_params, replication_slot:, use_temporary_slot:)
              .replicate_forever(Wal::LoggingWatcher.new(replication_slot, watcher), publications:)
            if auto_restart
              backoff_time = backoff_exponent ? (backoff * retries) ** backoff_exponent : backoff
              puts "Watcher finished for #{replication_slot}, auto restarting in #{backoff_time.floor(2)}..."
              sleep backoff_time
              puts "Restarting #{replication_slot}"
              redo
            end
          rescue ArgumentError
            raise
          rescue StandardError => err
            if retries < max_retries
              Wal.logger&.error("[#{replication_slot}] Error #{err}")
              Wal.logger&.error([err.message, *err.backtrace].join("\n"))
              retries += 1
              backoff_time = backoff_exponent ? (backoff * retries) ** backoff_exponent : backoff
              puts "Restarting #{replication_slot} in #{backoff_time.floor(2)}s..."
              sleep backoff_time
              puts "Restarting #{replication_slot}"
              retry
            end
            raise
          end

          puts "Watcher finished for #{replication_slot}"
          Process.kill("TERM", Process.pid)
        end
      end

      def setup_signal_handlers
        stop_threads = proc do
          puts "[#{name}] Stopping WAL threads..."
          @threads.each(&:kill)
          puts "[#{name}] WAL threads stopped"
          exit 0
        end

        Signal.trap("TERM", &stop_threads)
        Signal.trap("INT", &stop_threads)
      end
    end

    attr_reader :config, :db_config

    def initialize(config:, db_config:)
      @config = config
      @db_config = db_config
      @child_pids = []
    end

    def start
      workers_slots = config["slots"].group_by { |_slot, slot_config| slot_config["worker"] || "default" }

      if workers_slots.size == 1
        run_single_worker(workers_slots.first)
      else
        run_forked_workers(workers_slots)
      end
    end

    def run_single_worker((worker_name, slot_configs))
      @ping_thread = start_ping_thread
      puts "[#{worker_name}] Starting worker process (PID: #{Process.pid})"
      worker = Worker.new(name: worker_name, slot_configs: slot_configs, db_config: db_config)
      worker.run
    end

    def run_forked_workers(workers_slots)
      Wal.fork_hooks[:before_fork]&.call

      workers_slots.each do |worker_name, slot_configs|
        pid = fork_worker(worker_name, slot_configs)
        @child_pids << pid
        puts "Spawned worker '#{worker_name}' with PID #{pid}"
      end

      @ping_thread = start_ping_thread
      setup_signal_handlers
      wait_for_workers
    end

    private

    def fork_worker(worker_name, slot_configs)
      Process.fork do
        Wal.fork_hooks[:after_fork]&.call
        puts "[#{worker_name}] Starting worker process (PID: #{Process.pid})"
        worker = Worker.new(name: worker_name, slot_configs: slot_configs, db_config: db_config)
        worker.run
      end
    end

    def start_ping_thread
      Thread.new do
        loop do
          ActiveRecord::Base.connection_pool.with_connection do |conn|
            conn.execute("SELECT pg_logical_emit_message(true, 'wal_ping', '{}')")
          end
          sleep 20
        end
      end
    end

    def setup_signal_handlers
      stop_workers = proc do
        puts "Stopping all worker processes..."
        @ping_thread&.kill
        @child_pids.each do |pid|
          Process.kill("TERM", pid)
        rescue Errno::ESRCH
          # Process already dead
        end
        @child_pids.each do |pid|
          Process.wait(pid)
        rescue Errno::ECHILD
          # Already reaped
        end
        puts "All worker processes stopped"
        exit 0
      end

      Signal.trap("TERM", &stop_workers)
      Signal.trap("INT", &stop_workers)

      @stop_workers = stop_workers
    end

    def wait_for_workers
      loop do
        exited_pid = Process.wait(-1)
        if @child_pids.include?(exited_pid)
          @child_pids.delete(exited_pid)
          puts "Worker process #{exited_pid} exited unexpectedly, shutting down..."
          @stop_workers.call
        end
      rescue Errno::ECHILD
        puts "All worker processes have exited"
        break
      end
    end
  end
end
