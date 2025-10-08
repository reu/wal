require "testcontainers/postgres"
require "debug"
require "minitest/mock"
require "wal"

RSpec.configure do |config|
  config.expect_with :minitest

  config.mock_with :rspec do |mocks|
    mocks.verify_partial_doubles = true
  end

  config.shared_context_metadata_behavior = :apply_to_host_groups

  config.add_setting :postgres_container, default: nil
  config.add_setting :pg_config, default: nil

  config.before(:suite) do
    pg_container = config.postgres_container = Testcontainers::PostgresContainer
      .new
      .with_command(["-cwal_level=logical", "-cmax_wal_senders=500", "-cmax_replication_slots=500"])
      .start

    pg_config = config.pg_config = {
      database: pg_container.username,
      host: pg_container.host,
      username: pg_container.username,
      password: pg_container.password,
      port: pg_container.first_mapped_port,
    }

    ActiveRecord::Base.establish_connection(**pg_config.merge(adapter: "postgresql"))
    ActiveRecord::Schema.define do
      create_table :records, force: true do |t|
        t.string :name
      end
      execute("ALTER TABLE records REPLICA IDENTITY FULL")

      execute("CREATE SCHEMA alternate")
      create_table "alternate.records", force: true do |t|
        t.string :name
      end
      execute("ALTER TABLE alternate.records REPLICA IDENTITY FULL")

      execute("CREATE PUBLICATION debug_publication FOR ALL TABLES")
    end

    class Record < ActiveRecord::Base; end
  end

  config.after(:suite) do
    config.postgres_container&.stop
    config.postgres_container&.remove
  end

  module ReplicationHelpers
    def create_testing_wal_replication(watcher, db_config: nil, publications: ["debug_publication"])
      Wal::Replicator
        .new(
          replication_slot: "wal_replicator_test_#{SecureRandom.alphanumeric(8)}",
          use_temporary_slot: true,
          db_config: db_config || RSpec.configuration.pg_config,
        )
        .replicate(watcher, publications:)
    end

    def replicate_single_transaction(replication_stream)
      Enumerator::Lazy
        .new(replication_stream) do |yielder, event|
          yielder.yield(event)
          raise StopIteration if event.is_a? Wal::CommitTransactionEvent
        end
        .force
    end
  end

  config.include ReplicationHelpers
end
