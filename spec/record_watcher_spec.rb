require "spec_helper"

RSpec.describe Wal::RecordWatcher do
  class BufferRecordWatcher < Wal::RecordWatcher
    attr_reader :change_events

    def initialize(aggregation_strategy: :memory)
      @change_events = []
      @aggregation_strategy = aggregation_strategy
    end

    def clear!
      @change_events = []
    end

    def on_record_changed(event)
      @change_events << event
    end

    def aggregation_strategy(*)
      @aggregation_strategy
    end

    def should_watch_table?(table)
      true
    end
  end

  %i[memory temporary_table].each do |aggregation_strategy|
    context "using #{aggregation_strategy} aggregation strategy" do
      let(:watcher) { BufferRecordWatcher.new(aggregation_strategy:) }

      it "reports insert changes" do
        replication = create_testing_wal_replication(watcher)

        record = ActiveRecord::Base.transaction do
          record = Record.create!(name: "OriginalName")

          # Even we updating the event, we should still report as a insert
          record.name = "UpdatedName"
          record.save!
          record
        end

        replicate_single_transaction(replication)

        assert_equal 1, watcher.change_events.size
        assert_instance_of Wal::InsertEvent, watcher.change_events[0]
        assert_equal record.id, watcher.change_events[0].primary_key
        assert_equal "UpdatedName", watcher.change_events[0].attribute("name")

      ensure
        record&.delete
      end

      it "reports update changes" do
        record = Record.create!(name: "OriginalName")

        replication = create_testing_wal_replication(watcher)

        ActiveRecord::Base.transaction do
          record.name = "UpdatedName"
          record.save!

          record.name = "AnotherUpdatedName"
          record.save!
        end

        replicate_single_transaction(replication)

        assert_equal 1, watcher.change_events.size
        assert_instance_of Wal::UpdateEvent, watcher.change_events[0]
        assert_equal record.id, watcher.change_events[0].primary_key
        assert_equal "AnotherUpdatedName", watcher.change_events[0].attribute("name")

      ensure
        record&.delete
      end

      it "reports delete changes" do
        record = Record.create!(name: "OriginalName")

        replication = create_testing_wal_replication(watcher)

        ActiveRecord::Base.transaction do
          record.name = "UpdatedName"
          record.save!
          record.destroy!
        end

        replicate_single_transaction(replication)

        assert_equal 1, watcher.change_events.size
        assert_instance_of Wal::DeleteEvent, watcher.change_events[0]
        assert_equal record.id, watcher.change_events[0].primary_key
        assert_equal "OriginalName", watcher.change_events[0].attribute_was("name")
        assert_nil watcher.change_events[0].attribute("name")

      ensure
        record&.delete
      end

      it "doesn't report changes when the record is created and deleted on the same transaction" do
        replication = create_testing_wal_replication(watcher)

        record = ActiveRecord::Base.transaction do
          record = Record.create!(name: "OriginalName")
          record.name = "UpdatedName"
          record.save!
          record.destroy!
        end

        replicate_single_transaction(replication)

        assert_empty watcher.change_events
      end

      it "proper supports multiple records" do
        replication = create_testing_wal_replication(watcher)

        record1, record2 = ActiveRecord::Base.transaction do
          [
            Record.create!(name: "Record1"),
            Record.create!(name: "Record2"),
          ]
        end

        replicate_single_transaction(replication)

        assert_equal 2, watcher.change_events.size
        assert_includes watcher.change_events.map { |evt| evt.attribute("name") }, "Record1"
        assert_includes watcher.change_events.map { |evt| evt.attribute("name") }, "Record2"

        replication = create_testing_wal_replication(watcher)

        ActiveRecord::Base.transaction do
          record1.update_column(:name, "NewName1")
          record2.update_column(:name, "NewName2")
        end

        watcher.clear!
        replicate_single_transaction(replication)

        assert_equal 2, watcher.change_events.size
        assert_includes watcher.change_events.map { |evt| evt.attribute("name") }, "NewName1"
        assert_includes watcher.change_events.map { |evt| evt.attribute("name") }, "NewName2"
      ensure
        record1&.delete
        record2&.delete
      end

      it "supports on insert events via DSL" do
        mock = Minitest::Mock.new

        watcher = Class.new(Wal::RecordWatcher) do
          on_insert(Record) { |event| mock.on_save(event) }

          define_method(:aggregation_strategy) do |_|
            aggregation_strategy
          end
        end

        replication = create_testing_wal_replication(watcher.new)

        record = ActiveRecord::Base.transaction do
          Record.create!(name: "Record")
        end

        mock.expect(:on_save, true) do |event|
          event.is_a?(Wal::InsertEvent) && event.primary_key == record.id
        end

        replicate_single_transaction(replication)

        assert mock.verify
      ensure
        record&.delete
      end

      it "supports on update events via DSL" do
        mock = Minitest::Mock.new

        watcher = Class.new(Wal::RecordWatcher) do
          on_update(Record, changed: [:name]) { |event| mock.on_save(event) }

          define_method(:aggregation_strategy) do |_|
            aggregation_strategy
          end
        end

        record = Record.create!(name: "Record1")

        replication = create_testing_wal_replication(watcher.new)

        ActiveRecord::Base.transaction do
          record.name = "NewRecord1"
          record.save!
        end

        mock.expect(:on_save, true) do |event|
          event.is_a?(Wal::UpdateEvent) && event.primary_key == record.id
        end

        replicate_single_transaction(replication)

        assert mock.verify
      ensure
        record&.delete
      end

      it "supports on delete events via DSL" do
        mock = Minitest::Mock.new

        watcher = Class.new(Wal::RecordWatcher) do
          on_delete(Record) { |event| mock.on_delete(event) }

          define_method(:aggregation_strategy) do |_|
            aggregation_strategy
          end
        end

        record = Record.create!(name: "Record1")

        replication = create_testing_wal_replication(watcher.new)

        ActiveRecord::Base.transaction do
          record.destroy!
        end

        mock.expect(:on_delete, true) do |event|
          event.is_a?(Wal::DeleteEvent) && event.primary_key == record.id
        end

        replicate_single_transaction(replication)

        assert mock.verify
      ensure
        record&.delete
      end
    end
  end
end
