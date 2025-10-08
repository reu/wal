require "ostruct"

module Wal
  # Responsible to hook into a Postgres logical replication slot and stream the changes to a specific `Watcher`.
  # Also it supports inject "contexts" into the replication events.
  class Replicator
    include PG::Replication::Protocol

    def initialize(
      replication_slot:,
      use_temporary_slot: false,
      db_config: ActiveRecord::Base.configurations.configs_for(name: "primary").configuration_hash
    )
      @db_config = db_config
      @replication_slot = replication_slot
      @use_temporary_slot = use_temporary_slot
    end

    def replicate_forever(watcher, publications:)
      replication = replicate(watcher, publications:)
      loop { replication.next }
    rescue StopIteration
      nil
    end

    def replicate(watcher, publications:)
      watch_conn = PG.connect(
        dbname: @db_config[:database],
        host: @db_config[:host],
        user: @db_config[:username],
        password: @db_config[:password].presence,
        port: @db_config[:port].presence,
        replication: "database",
      )

      begin
        watch_conn.query(<<~SQL)
          CREATE_REPLICATION_SLOT #{@replication_slot} #{@use_temporary_slot ? "TEMPORARY" : ""} LOGICAL "pgoutput"
        SQL
      rescue PG::DuplicateObject
        # We are fine, we already have created this slot in a previous run
      end

      tables = {}
      context = {}
      transaction_id = nil

      watch_conn.start_pgoutput_replication_slot(@replication_slot, publications, messages: true).filter_map do |msg|
        case msg
        in XLogData(data: PG::Replication::PGOutput::Relation(oid:, name:, columns:, namespace:))
          tables[oid] = Table.new(
            # TODO: for now we are forcing an id column here, but that is not really correct
            primary_key_colums: columns.any? { |col| col.name == "id" } ? ["id"] : [],
            schema: namespace,
            name:,
            columns: columns.map do |col|
              Column.new(
                name: col.name,
                decoder: ActiveRecord::Base.connection.lookup_cast_type_from_column(
                  # We have to create this OpenStruct because weird AR API reasons...
                  # And the `sql_type` param luckly doesn't really matter for our use case
                  ::OpenStruct.new(oid: col.oid, fmod: col.modifier, sql_type: "")
                ),
              )
            end
          )
          next

        in XLogData(lsn:, data: PG::Replication::PGOutput::Begin(xid:, timestamp:, final_lsn:))
          transaction_id = xid
          context = {}
          BeginTransactionEvent.new(
            transaction_id:,
            lsn:,
            final_lsn:,
            timestamp:,
          ).tap { |event| watcher.on_event(event) }

        in XLogData(lsn:, data: PG::Replication::PGOutput::Commit(timestamp:))
          CommitTransactionEvent.new(
            transaction_id:,
            lsn:,
            context:,
            timestamp:,
          ).tap do |event|
            watcher.on_event(event)
            watch_conn.standby_status_update(write_lsn: lsn)
          end

        in XLogData(lsn:, data: PG::Replication::PGOutput::Message(prefix: "wal_ping"))
          watch_conn.standby_status_update(write_lsn: [watch_conn.last_confirmed_lsn, lsn].compact.max)
          next

        in XLogData(data: PG::Replication::PGOutput::Message(prefix:, content:)) if watcher.valid_context_prefix? prefix
          begin
            context = JSON.parse(content).presence || {}
            next
          rescue JSON::ParserError
            # Invalid context received, just ignore
          end

        in XLogData(lsn:, data: PG::Replication::PGOutput::Insert(oid:, new:))
          table = tables[oid]
          next unless watcher.should_watch_table? table.full_table_name
          new_data = table.decode_row(new)
          record_id = table.primary_key(new_data)
          next unless record_id

          InsertEvent.new(
            transaction_id:,
            lsn:,
            context:,
            schema: table.schema,
            table: table.name,
            primary_key: record_id,
            new: new_data,
          ).tap { |event| watcher.on_event(event) }

        in XLogData(lsn:, data: PG::Replication::PGOutput::Update(oid:, new:, old:))
          table = tables[oid]
          next unless watcher.should_watch_table? table.full_table_name
          old_data = table.decode_row(old)
          new_data = table.decode_row(new)
          record_id = table.primary_key(new_data)
          next unless record_id

          UpdateEvent.new(
            transaction_id:,
            lsn:,
            context:,
            schema: table.schema,
            table: table.name,
            primary_key: record_id,
            old: old_data,
            new: new_data,
          ).tap { |event| watcher.on_event(event) }

        in XLogData(lsn:, data: PG::Replication::PGOutput::Delete(oid:, old:, key:))
          table = tables[oid]
          next unless watcher.should_watch_table? table.full_table_name
          old_data = table.decode_row(old.presence || key)
          record_id = table.primary_key(old_data)
          next unless record_id

          DeleteEvent.new(
            transaction_id:,
            lsn:,
            context:,
            schema: table.schema,
            table: table.name,
            primary_key: record_id,
            old: old_data,
          ).tap { |event| watcher.on_event(event) }

        else
          next
        end
      end
    end

    class Column < Data.define(:name, :decoder)
      def decode(value)
        decoder.deserialize(value)
      end
    end

    class Table < Data.define(:schema, :name, :primary_key_colums, :columns)
      def full_table_name
        case schema
        in "public"
          name
        else
          "#{schema}.#{name}"
        end
      end

      def primary_key(decoded_row)
        case primary_key_colums
        in [key]
          case decoded_row[key]
          in Integer => id
            id
          in String => id
            id
          else
            # Only supporting string and integer primary keys for now
            nil
          end
        else
          # Not supporting coumpound primary keys
          nil
        end
      end

      def decode_row(values)
        values
          .zip(columns)
          .map { |tuple, col| [col.name, col.decode(tuple.data)] }
          .to_h
      end
    end
  end
end
