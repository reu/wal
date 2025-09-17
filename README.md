# Wal

Wal is a framework that lets you hook into Postgres WAL events directly from your Rails application.

Unlike using database triggers, Wal allows you to keep your logic in your application code while still reacting to persistence events coming from the database.

Also, unlike ActiveRecord callbacks, these events are guaranteed by Postgres to be 100% consistent, ensuring you never miss one.

# Getting started

## Installation

Add `wal` to your application's Gemfile:

```ruby
gem "wal"
```

And then:

```bash
$ bundle install
```

## Getting started

The core building block in Wal is a `Watcher`. The easiest way to create one is by extending `Wal::RecordWatcher`, which handles most of the boilerplate for you.

For example, let's create a watcher that denormalizes `Post` and `Category` models into a `DenormalizedPost`.

Create a new file at `app/watchers/denormalize_post_watcher.rb`:

```ruby
class DenormalizePostWatcher < Wal::RecordWatcher
  # When a new `Post` is created, we create a new `DenormalizedPost` record
  on_insert Post do |event|
    DenormalizedPost.create!(
      post_id: event.primary_key,
      title: event.new["title"],
      body: event.new["body"],
      category_id: event.new["category_id"],
      category_name: Category.find_by(id: event.new["category_id"])&.name,
    )
  end

  # When a `Post` title or body is changed, we update its `DenormalizedPost` record
  on_update Post, changed: [:title, :body] do |event|
    DenormalizedPost
      .where(post_id: event.primary_key)
      .update_all(
        title: event.new["title"],
        body: event.new["body"],
      )
  end

  # When a `Post` category changes, we also update its `DenormalizedPost` record
  on_update Post, changed: [:category_id] do |event|
    DenormalizedPost
      .where(post_id: event.primary_key)
      .update_all(
        category_id: event.new["category_id"],
        category_name: Category.find_by(id: event.new["category_id"])&.name,
      )
  end

  # When a `Category` changes, we update all the `DenormalizedPosts` referencing it
  on_update Category, changed: [:name] do |event|
    DenormalizedPost
      .where(category_id: event.primary_key)
      .update_all(
        category_name: event.new["name"],
      )
  end

  # Finally when a `Category` is deleted, we clear all the `DenormalizedPosts` referencing it
  on_delete Category do |event|
    DenormalizedPost
      .where(category_id: event.primary_key)
      .update_all(
        category_id: nil,
        category_name: nil,
      )
  end
end
```

You might wonder: *Why not just use ActiveRecord callbacks for this?*

And while it is hard to justify that for our simple example, ActiveRecord callbacks are not guaranteed to always run. Depending on the methods you use to perform the changes, they can be skipped.

Wal ensures every single change is captured. *Even when updates happen directly in the database and bypass Rails entirely*. That's the main reason to use it: when you need 100% consistency.

Usually one could resort into database triggers when full consistency is required, but running and maintaining application level code on the database tends to be painful. Wal let's you do the same but at the application level.

## Configuring the Watcher

Wal relies on [Postgres logical replication](https://www.postgresql.org/docs/current/logical-replication.html) to stream changes to your watchers.

First, create a [Postgres publication](https://www.postgresql.org/docs/current/logical-replication-publication.html) for the tables your watcher uses. Wal provides a generator for this:

```
$ rails generate wal:migration DenormalizePostWatcher
```

This will generate a new migration with all the tables that your watcher uses:
```ruby
class SetDenormalizePostWatcherPublication < ActiveRecord::Migration
  def change
    define_publication :denormalize_post_publication do |p|
      p.table :posts
      p.table :categories
    end
  end
end
```

Next, create a `config/wal.yml` configuration file to link the `Watcher` to its publication:

```yaml
slots:
  denormalize_posts:
    watcher: DenormalizePostWatcher
    publications:
      - denormalize_post_publication
```
This associates your watcher with the `denormalize_post_publication` and with the `denormalize_posts` [Postgres replication slot](https://www.postgresql.org/docs/9.4/warm-standby.html#STREAMING-REPLICATION-SLOTS).

## Running the Watcher

With everything configured, start the Wal process:

```bash
bundle exec wal start config/wal.yaml
```

Wal will now process your replication slot and run the `DenormalizePostWatcher` whenever a change occur.
