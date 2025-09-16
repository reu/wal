# Wal

Easily hook into Postgres WAL event log from your Rails app.

Proper documentation TBD

## Examples

### Watch for model changes using the RecordWatcher DSL

```ruby
class ProductAvailabilityWatcher < Wal::RecordWatcher
  on_save Product, changed: %w[price] do |event|
    recalculate_inventory_price(event.primary_key, event.new["price"])
  end

  on_delete Product do |event|
    clear_product_inventory(event.primary_key)
  end

  on_save Sales, changed: %w[status] do |event|
    recalculate_inventory_quantity(event.primary_key)
  end

  def recalculate_inventory_price(product_id, new_price)
    # ...
  end

  def clear_product_inventory(product_id)
    # ...
  end

  def recalculate_inventory_quantity(sales_id)
    # ...
  end
end
```

### Basic watcher implementation

```ruby
class LogWatcher
  include Wal::Watcher

  def on_event(event)
    puts "Wal event received #{event}"
  end
end
```
