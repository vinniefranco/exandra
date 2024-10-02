# Exandra

[![Hex.pm](https://img.shields.io/hexpm/v/exandra)](https://hex.pm/packages/exandra)
![GitHub](https://img.shields.io/github/license/vinniefranco/exandra)
[![CI](https://github.com/vinniefranco/exandra/actions/workflows/main.yml/badge.svg)](https://github.com/vinniefranco/exandra/actions/workflows/main.yml)
[![Coverage Status](https://coveralls.io/repos/github/vinniefranco/exandra/badge.svg?branch=main)](https://coveralls.io/github/vinniefranco/exandra?branch=main)
![Libraries.io dependency status for the latest release](https://img.shields.io/librariesio/release/hex/exandra)

Adapter module for [Apache Cassandra](https://cassandra.apache.org/_/index.html)
and [ScyllaDB](https://www.scylladb.com/).

Uses [`Xandra`](https://github.com/lexhide/xandra) for communication with the
underlying database.

Uses [`Ecto`](https://github.com/elixir-ecto/ecto) for database interfacing,
running schema migrations, and querying operations.

## Configuration

To configure an `Ecto.Repo` that uses `Exandra` as its adapter, you can use
the application configuration or pass the options when starting the repo.

You can use the following options:

* Any of the options supported by `Ecto.Repo` itself, which you can see
  in the `Ecto.Repo` documentation.

* Any of the options supported by `Xandra.Cluster.start_link/1`.

#{Exandra.Connection.start_opts_docs()}

To configure your Ecto repository to use this adapter, you can use the
`:adapter` option. For example, when defining the repo:

```elixir
defmodule MyApp.Repo do
  use Ecto.Repo, otp_app: :my_app, adapter: Exandra
end
```

You can configure your database connection in `config/dev.exs`. Here's an example `dev configuration`:

```elixir
# Configure your database
config :my_app, MyApp.Repo,
  migration_primary_key: [name: :id, type: :uuid], # Overrides the default type `bigserial` used for version attribute in schema migration
  nodes: ["127.0.0.1"],  # List of database connection endpoints
  keyspace: "my_app_dev", # Name of your keyspace
  sync_connect: 5000,             # Waiting time in milliseconds for the database connection
  log: :info,
  stacktrace: true,
  show_sensitive_data_on_connection_error: true,
  pool_size: 10

```

**Note:** The `bigserial` data type is specific to PostgreSQL databases and is not present in Scylla/Cassandra.

## Schemas

You can regularly use `Ecto.Schema` with Exandra. For example:

```elixir
defmodule User do
  use Ecto.Schema

  @primary_key {:id, Ecto.UUID, autogenerate: true}
  schema "users" do
    field :email, :string
    field :meta, Exandra.Map, key: :string, value: :string
  end
end
```

You can use all the usual types (`:string`, `Ecto.UUID`, and so on).

### Maps

The `:map` type gets stored in Cassandra/Scylla as a blob of text with the map encoded as
JSON. For example, if you have a schema with

```elixir
field :features, :map
```

you can pass the field as an Elixir map when setting it, and Exandra will convert it to a map
on the way from the database. Because Exandra uses JSON for this, you'll have to pay attention
to things such as atom keys (which can be used when writing, but will be strings when reading)
and such.

### User-Defined Types (UDTs)

If one of your fields is a UDT, you can use the `Exandra.UDT` type for it. For example, if you
have a `phone_number` UDT, you can declare fields with that type as:

```elixir
field :home_phone, Exandra.UDT, type: :phone_number
field :office_phone, Exandra.UDT, type: :phone_number
```

> #### String Keys {: .warning}
>
> There is no validation with `Exandra.UDT` and the keys _must_ be strings.

Alternatively, you can use the `Exandra.EmbeddedType` for `Ecto.Schema`-backed UDTs. For example, if you have a
`phone_number` UDT, you can use:

```elixir
field :home_phone, Exandra.EmbeddedType, using: MyApp.PhoneSchema
```

Finally, if you have a column of frozen UDTs `list<frozen<phone_number>>`, you can still use the
`Exandra.EmbeddedType` just as before with `cardinality: :many` like so:

```elixir
field :home_phone, Exandra.EmbeddedType, cardinality: :many, using: MyApp.PhoneSchema
```

### Inets

Cassandra/Scylla has a native `inet` type which represents either an ipv4 or an ipv6 address.
Exandra provides the `Exandra.Inet` type for these fields.

```elixir
field :last_ip, Exandra.Inet
```

### Tuples

Tuples can be declared using the `Exandra.Tuple` type.

```elixir
field :version, Exandra.Tuple, types: [:integer, :integer, :integer]
```

### Native Collections (Lists, Maps, Sets)

Access to native Cassandra/Scylla collections is available
using the appropriate data types in the field definition.

```elixir
@primary_key false
schema "hotels" do
  field :checkins, {:array, :utc_datetime}                            # list<timestamp>
  field :room_to_customer, Exandra.Map, key: :integer, value: :string # map<int, string>
  field :available_rooms, Exandra.Set, type: :integer                 # set<int>
end
```

> #### Composite Collections {: .tip}
>
> It's possible to create composite collections using the following syntax:
>
> ```elixir
> field :complex_type, {:array, Exandra.Map}, key: :string, value: Exandra.Set, type: :integer
> ```
>
> This creates a `list<map<tuple<integer>, set<string>>>`.
>
> However, please note that due to the limited expressivity of this representation,
> each collection at the same level will have the same typing information:
>
> ```elixir
> field :valid, Exandra.Set, type: Exandra.Set, type: :integer                                     # set<set<int>>
> field :invalid, Exandra.Map, key: Exandra.Set, type: :integer, value: Exandra.Set, type: :string # map<set<int>, set<int>> not map<set<int>, set<string>>
> ```

### Counter Tables

You can use the `Exandra.Counter` type to create counter fields (in counter tables). For
example:

```elixir
@primary_key false
schema "page_views" do
  field :route, :string, primary_key: true
  field :total, Exandra.Counter
end
```
You can only *update* counter fields. You'll have to use `c:Ecto.Repo.update_all/2`
to insert or update counters. For example, in the table above, you'd update the
`:total` counter field with:

```elixir
query =
  from page_view in "page_views",
    where: page_view.route == "/browse",
    update: [set: [total: 1]]

MyApp.Repo.update_all(query)
```

## Batch Queries

You can run **batch queries** through Exandra. Batch queries are supported by
Cassandra/Scylla, and allow you to run multiple queries in a single request.
See `Exandra.Batch` for more information and examples.

## Multiple keyspaces using prefixes

You can use [query prefixes](https://hexdocs.pm/ecto/multi-tenancy-with-query-prefixes.html) to
query different keyspaces using the same schemas. As pointed out in the Ecto docs,
migrations must be run for *each* prefix in this case.

## Batch Queries

You can run **batch queries** through Exandra. Batch queries are supported by
Cassandra/Scylla, and allow you to run multiple queries in a single request.
See `Exandra.Batch` for more information and examples.

## Multiple keyspaces using prefixes

You can use [query prefixes](https://hexdocs.pm/ecto/multi-tenancy-with-query-prefixes.html) to
query different keyspaces using the same schemas. As pointed out in the Ecto docs,
migrations must be run for *each* prefix in this case.

## Migrations

You can use Exandra to run migrations as well, as it supports most of the DDL-related
commands from `Ecto.Migration`. For example:

```elixir
defmodule AddUsers do
  use Ecto.Migration

  def change do
    create table("users", primary_key: false) do
      add :email, :string, primary_key: true
      add :age, :int
    end
  end
end
```

> #### Cassandra and Scylla Types {: .info}
>
> When writing migrations, remember that you must use the **actual types** from Cassandra or
> Scylla, which you must pass in as an *atom*.
>
> For example, to add a column with the type of
> a map of integer keys to boolean values, you need to declare its type as
> `:"map<int, boolean>"`.

This is a non-comprehensive list of types you can use:

* `:"map<key_type, value_type>"` - maps (such as `:"map<int, boolean>"`).
* `:"list<type>"` - lists (such as `:"list<uuid>"`).
* `:string` - gets translated to the `text` type.
* `:map` - maps get stored as text, and Exandra dumps and loads them automatically.
* `<udt>` - User-Defined Types (UDTs) should be specified as their name, expressed as an
  atom. For example, a UDT called `full_name` would be specified as the type `:full_name`.
* `:naive_datetime`, `:naive_datetime_usec`, `:utc_datetime`, `:utc_datetime_usec` -
  these are all represented as the `timestamp` type.

### User-Defined Types (UDTs)

`Ecto.Migration` doesn't support creating, altering, or dropping Cassandra/Scylla **UDTs**.
To do those operations in a migration, use `Ecto.Migration.execute/1`
or `Ecto.Migration.execute/2`. For example, in your migration module:

```elixir
def change do
  execute(
    _up_query = "CREATE TYPE full_name (first_name text, last_name text))",
    _down_query = "DROP TYPE full_name"
  )
end
```

