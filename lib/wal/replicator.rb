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
      @primary_key_cache = {}
    end

    def replicate_forever(watcher, publications:)
      replication = replicate(watcher, publications:)
      loop { replication.next }
    rescue StopIteration
      nil
    end

    def close
      @watch_conn&.stop_replication
      @watch_conn&.close
      @watch_conn = nil
      @metadata_conn&.close
      @metadata_conn = nil
    end

    def replicate(watcher, publications:)
      @watch_conn = PG.connect(
        dbname: @db_config[:database],
        host: @db_config[:host],
        user: @db_config[:username],
        password: @db_config[:password].presence,
        port: @db_config[:port].presence,
        replication: "database",
      )

      @metadata_conn = connect_metadata

      begin
        @watch_conn.query(<<~SQL)
          CREATE_REPLICATION_SLOT #{@replication_slot} #{@use_temporary_slot ? "TEMPORARY" : ""} LOGICAL "pgoutput"
        SQL
      rescue PG::DuplicateObject
        # We are fine, we already have created this slot in a previous run
      end

      tables = {}
      context = {}
      transaction_id = nil

      @watch_conn.start_pgoutput_replication_slot(@replication_slot, publications, messages: true).filter_map do |msg|
        case msg
        in XLogData(data: PG::Replication::PGOutput::Relation(oid:, name:, columns:, namespace:))
          tables[oid] = Table.new(
            primary_key_columns: fetch_primary_key_columns(namespace, name),
            schema: namespace,
            name:,
            columns: columns.map { |col| Column.new(oid: col.oid, name: col.name) },
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
            @watch_conn.standby_status_update(write_lsn: lsn)
          end

        in XLogData(lsn:, data: PG::Replication::PGOutput::Message(prefix: "wal_ping"))
          @watch_conn.standby_status_update(write_lsn: [@watch_conn.last_confirmed_lsn, lsn].compact.max)
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
          new_data = table.decode_row(new)
          old_data = table.decode_row(old, new_data)
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
      rescue
        close
        raise
      end
    end

    private

    def connect_metadata
      PG.connect(
        dbname: @db_config[:database],
        host: @db_config[:host],
        user: @db_config[:username],
        password: @db_config[:password].presence,
        port: @db_config[:port].presence,
      )
    end

    def fetch_primary_key_columns(schema, table_name)
      result = @metadata_conn.exec_params(<<~SQL, [schema, table_name]).to_a.map { |row| row["attname"] }
        SELECT a.attname
        FROM pg_constraint c
        JOIN pg_class t ON c.conrelid = t.oid
        JOIN pg_namespace n ON t.relnamespace = n.oid
        JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(c.conkey)
        WHERE n.nspname = $1 AND t.relname = $2 AND c.contype = 'p'
        ORDER BY array_position(c.conkey, a.attnum)
      SQL

      return result if result.size > 0

      # Fallback to unique index columns
      result = @metadata_conn.exec_params(<<~SQL, [schema, table_name]).to_a
        SELECT a.attname, i.indexrelid::bigint
        FROM pg_index i
        JOIN pg_class t ON i.indrelid = t.oid
        JOIN pg_namespace n ON t.relnamespace = n.oid
        JOIN unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord) ON true
        JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = k.attnum
        WHERE n.nspname = $1 AND t.relname = $2 AND i.indisunique = true
        ORDER BY i.indisprimary DESC, i.indexrelid, k.ord
      SQL

      result
        .filter { |row| row["indexrelid"] == result.first["indexrelid"] }
        .map { |row| row["attname"] } if result.size > 0

    rescue PG::ConnectionBad
      @metadata_conn = connect_metadata
      retry
    end

    class Column < Data.define(:name, :oid)
      BOOLEAN_DECODER = PG::TextDecoder::Boolean.new
      BYTEA_DECODER = PG::TextDecoder::Bytea.new
      INTEGER_DECODER = PG::TextDecoder::Integer.new
      FLOAT_DECODER = PG::TextDecoder::Float.new
      NUMERIC_DECODER = PG::TextDecoder::Numeric.new
      DATE_DECODER = PG::TextDecoder::Date.new
      TIMESTAMP_DECODER = PG::TextDecoder::TimestampWithoutTimeZone.new
      TIMESTAMPTZ_DECODER = PG::TextDecoder::TimestampWithTimeZone.new
      JSON_DECODER = PG::TextDecoder::JSON.new
      INET_DECODER = PG::TextDecoder::Inet.new
      STRING_DECODER = PG::TextDecoder::String.new

      BOOLEAN_ARRAY_DECODER = PG::TextDecoder::Array.new(elements_type: BOOLEAN_DECODER)
      BYTEA_ARRAY_DECODER = PG::TextDecoder::Array.new(elements_type: BYTEA_DECODER)
      INTEGER_ARRAY_DECODER = PG::TextDecoder::Array.new(elements_type: INTEGER_DECODER)
      FLOAT_ARRAY_DECODER = PG::TextDecoder::Array.new(elements_type: FLOAT_DECODER)
      NUMERIC_ARRAY_DECODER = PG::TextDecoder::Array.new(elements_type: NUMERIC_DECODER)
      DATE_ARRAY_DECODER = PG::TextDecoder::Array.new(elements_type: DATE_DECODER)
      TIMESTAMP_ARRAY_DECODER = PG::TextDecoder::Array.new(elements_type: TIMESTAMP_DECODER)
      TIMESTAMPTZ_ARRAY_DECODER = PG::TextDecoder::Array.new(elements_type: TIMESTAMPTZ_DECODER)
      JSON_ARRAY_DECODER = PG::TextDecoder::Array.new(elements_type: JSON_DECODER)
      INET_ARRAY_DECODER = PG::TextDecoder::Array.new(elements_type: INET_DECODER)
      STRING_ARRAY_DECODER = PG::TextDecoder::Array.new(elements_type: STRING_DECODER)

      # Reference: https://github.com/postgres/postgres/blob/master/src/include/catalog/pg_type.dat
      DECODERS = {
        16 => BOOLEAN_DECODER,         # bool
        1000 => BOOLEAN_ARRAY_DECODER, # _bool

        17 => BYTEA_DECODER,         # bytea
        1001 => BYTEA_ARRAY_DECODER, # _bytea

        20 => INTEGER_DECODER,         # int8
        21 => INTEGER_DECODER,         # int2
        23 => INTEGER_DECODER,         # int4
        26 => INTEGER_DECODER,         # oid
        28 => INTEGER_DECODER,         # xid
        29 => INTEGER_DECODER,         # cid
        5069 => INTEGER_DECODER,       # xid8
        1005 => INTEGER_ARRAY_DECODER, # _int2
        1007 => INTEGER_ARRAY_DECODER, # _int4
        1016 => INTEGER_ARRAY_DECODER, # _int8
        1028 => INTEGER_ARRAY_DECODER, # _oid
        1011 => INTEGER_ARRAY_DECODER, # _xid
        1012 => INTEGER_ARRAY_DECODER, # _cid
        271 => INTEGER_ARRAY_DECODER,  # _xid8

        700 => FLOAT_DECODER,        # float4
        701 => FLOAT_DECODER,        # float8
        1021 => FLOAT_ARRAY_DECODER, # _float4
        1022 => FLOAT_ARRAY_DECODER, # _float8

        1700 => NUMERIC_DECODER,       # numeric
        790 => NUMERIC_DECODER,        # money
        1231 => NUMERIC_ARRAY_DECODER, # _numeric
        791 => NUMERIC_ARRAY_DECODER,  # _money

        1082 => DATE_DECODER,              # date
        1114 => TIMESTAMP_DECODER,         # timestamp
        1184 => TIMESTAMPTZ_DECODER,       # timestamptz
        1083 => STRING_DECODER,            # time (no dedicated decoder, use string)
        1266 => STRING_DECODER,            # timetz
        1186 => STRING_DECODER,            # interval
        1182 => DATE_ARRAY_DECODER,        # _date
        1115 => TIMESTAMP_ARRAY_DECODER,   # _timestamp
        1185 => TIMESTAMPTZ_ARRAY_DECODER, # _timestamptz
        1183 => STRING_ARRAY_DECODER,      # _time
        1270 => STRING_ARRAY_DECODER,      # _timetz
        1187 => STRING_ARRAY_DECODER,      # _interval

        114 => JSON_DECODER,          # json
        3802 => JSON_DECODER,         # jsonb
        199 => JSON_ARRAY_DECODER,    # _json
        3807 => JSON_ARRAY_DECODER,   # _jsonb
        4072 => STRING_DECODER,       # jsonpath
        4073 => STRING_ARRAY_DECODER, # _jsonpath

        869 => INET_DECODER,          # inet
        650 => INET_DECODER,          # cidr
        829 => STRING_DECODER,        # macaddr
        774 => STRING_DECODER,        # macaddr8
        1041 => INET_ARRAY_DECODER,   # _inet
        651 => INET_ARRAY_DECODER,    # _cidr
        1040 => STRING_ARRAY_DECODER, # _macaddr
        775 => STRING_ARRAY_DECODER,  # _macaddr8

        18 => STRING_DECODER,         # char
        19 => STRING_DECODER,         # name
        25 => STRING_DECODER,         # text
        1042 => STRING_DECODER,       # bpchar
        1043 => STRING_DECODER,       # varchar
        2950 => STRING_DECODER,       # uuid
        142 => STRING_DECODER,        # xml
        1002 => STRING_ARRAY_DECODER, # _char
        1003 => STRING_ARRAY_DECODER, # _name
        1009 => STRING_ARRAY_DECODER, # _text
        1014 => STRING_ARRAY_DECODER, # _bpchar
        1015 => STRING_ARRAY_DECODER, # _varchar
        2951 => STRING_ARRAY_DECODER, # _uuid
        143 => STRING_ARRAY_DECODER,  # _xml

        1560 => STRING_DECODER,       # bit
        1562 => STRING_DECODER,       # varbit
        1561 => STRING_ARRAY_DECODER, # _bit
        1563 => STRING_ARRAY_DECODER, # _varbit

        600 => STRING_DECODER,        # point
        601 => STRING_DECODER,        # lseg
        602 => STRING_DECODER,        # path
        603 => STRING_DECODER,        # box
        604 => STRING_DECODER,        # polygon
        628 => STRING_DECODER,        # line
        718 => STRING_DECODER,        # circle
        1017 => STRING_ARRAY_DECODER, # _point
        1018 => STRING_ARRAY_DECODER, # _lseg
        1019 => STRING_ARRAY_DECODER, # _path
        1020 => STRING_ARRAY_DECODER, # _box
        1027 => STRING_ARRAY_DECODER, # _polygon
        629 => STRING_ARRAY_DECODER,  # _line
        719 => STRING_ARRAY_DECODER,  # _circle

        3904 => STRING_DECODER,       # int4range
        3906 => STRING_DECODER,       # numrange
        3908 => STRING_DECODER,       # tsrange
        3910 => STRING_DECODER,       # tstzrange
        3912 => STRING_DECODER,       # daterange
        3926 => STRING_DECODER,       # int8range
        4451 => STRING_DECODER,       # int4multirange
        4532 => STRING_DECODER,       # nummultirange
        4533 => STRING_DECODER,       # tsmultirange
        4534 => STRING_DECODER,       # tstzmultirange
        4535 => STRING_DECODER,       # datemultirange
        4536 => STRING_DECODER,       # int8multirange
        3905 => STRING_ARRAY_DECODER, # _int4range
        3907 => STRING_ARRAY_DECODER, # _numrange
        3909 => STRING_ARRAY_DECODER, # _tsrange
        3911 => STRING_ARRAY_DECODER, # _tstzrange
        3913 => STRING_ARRAY_DECODER, # _daterange
        3927 => STRING_ARRAY_DECODER, # _int8range

        3614 => STRING_DECODER,       # tsvector
        3615 => STRING_DECODER,       # tsquery
        3643 => STRING_ARRAY_DECODER, # _tsvector
        3645 => STRING_ARRAY_DECODER, # _tsquery

        3220 => STRING_DECODER,       # pg_lsn
        3221 => STRING_ARRAY_DECODER, # _pg_lsn

        24 => INTEGER_DECODER,         # regproc
        2202 => INTEGER_DECODER,       # regprocedure
        2203 => INTEGER_DECODER,       # regoper
        2204 => INTEGER_DECODER,       # regoperator
        2205 => INTEGER_DECODER,       # regclass
        2206 => INTEGER_DECODER,       # regtype
        4096 => INTEGER_DECODER,       # regrole
        4089 => INTEGER_DECODER,       # regnamespace
        3734 => INTEGER_DECODER,       # regconfig
        3769 => INTEGER_DECODER,       # regdictionary
        4191 => INTEGER_DECODER,       # regcollation
        1008 => INTEGER_ARRAY_DECODER, # _regproc
        2207 => INTEGER_ARRAY_DECODER, # _regprocedure
        2208 => INTEGER_ARRAY_DECODER, # _regoper
        2209 => INTEGER_ARRAY_DECODER, # _regoperator
        2210 => INTEGER_ARRAY_DECODER, # _regclass
        2211 => INTEGER_ARRAY_DECODER, # _regtype
        4097 => INTEGER_ARRAY_DECODER, # _regrole
        4090 => INTEGER_ARRAY_DECODER, # _regnamespace
        3735 => INTEGER_ARRAY_DECODER, # _regconfig
        3770 => INTEGER_ARRAY_DECODER, # _regdictionary
        4192 => INTEGER_ARRAY_DECODER, # _regcollation
      }.freeze

      def decode(value)
        value&.then { (DECODERS[oid] || STRING_DECODER).decode(_1) }
      end
    end

    class Table < Data.define(:schema, :name, :primary_key_columns, :columns)
      def full_table_name
        case schema
        in "public"
          name
        else
          "#{schema}.#{name}"
        end
      end

      def primary_key(decoded_row)
        return nil if primary_key_columns.empty?

        values = primary_key_columns.filter_map do |col_name|
          value = decoded_row[col_name]
          case value
          when Integer, String
            value
          else
            nil
          end
        end

        return nil if values.size != primary_key_columns.size

        values.size == 1 ? values.first : values
      end

      def decode_row(values, existing_data = {})
        values
          .zip(columns)
          .map { |tuple, col| [col.name, tuple.toast? ? existing_data[col.name] : col.decode(tuple.data)] }
          .to_h
      end
    end
  end
end
