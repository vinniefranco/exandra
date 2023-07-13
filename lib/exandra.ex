defmodule Exandra do
  @moduledoc """
  Adapter module for [Apache Cassandra](https://cassandra.apache.org/_/index.html)
  and [ScyllaDB](https://www.scylladb.com/).

  Uses [`Xandra`](https://github.com/lexhide/xandra) for communication with the
  underlying database.

  ## Configuration

  To configure an `Ecto.Repo` that uses `Exandra` as its adapter, you can use
  the application configuration or pass the options when starting the repo.

  You can use the following options:

    * Any of the options supported by `Ecto.Repo` itself, which you can see
      in the `Ecto.Repo` documentation.

    * Any of the option supported by `Xandra.Cluster.start_link/1`.

  #{Exandra.Connection.start_opts_docs()}

  To configure your Ecto repository to use this adapter, you can use the
  `:adapter` option. For example, when defining the repo:

      defmodule MyApp.Repo do
        use Ecto.Repo, otp_app: :my_app, adapter: Exandra
      end

  ## Schemas

  You can regularly use `Ecto.Schema` with Exandra. For example:

      defmodule User do
        use Ecto.Schema

        @primary_key {:id, :binary_id, autogenerate: true}
        schema "users" do
          field :email, :string
          field :meta, Exandra.Map, key: :string, value: :string
        end
      end

  You can use all the usual types (`:string`, `Ecto.UUID`, and so on).

  ### Maps

  The `:map` type gets stored in Cassandra/Scylla as a blob of text with the map encoded as
  JSON. For example, if you have a schema with

      field :features, :map

  you can pass the field as an Elixir map when setting it, and Exandra will convert it to a map
  on the way from the database. Because Exandra uses JSON for this, you'll have to pay attention
  to things such as atom keys (which can be used when writing, but will be strings when reading)
  and such.

  ### User-Defined Types (UDTs)

  If one of your fields is a UDT, you can use the `Exandra.UDT` type for it. For example, if you
  have a `phone_number` UDT, you can declare fields with that type as:

      field :home_phone, Exandra.UDT, type: :phone_number
      field :office_phone, Exandra.UDT, type: :phone_number

  ### Arrays

  You can use arrays with the Ecto `{:array, <type>}` type. This gets translated to the
  `list<_>` native Cassandra/Scylla type. For example, you can declare a field as

      field :checkins, {:array, :utc_datetime}

  This field will use the native type `list<timestamp>`.

  > #### Exandra Types {: .tip}
  >
  > If you want to use actual Cassandra/Scylla types such as `map<_, _>` or
  > `set<_>`, you can use the corresponding Exandra types `Exandra.Map` and `Exandra.Set`.

  ### Counter Tables

  You can use the `Exandra.Counter` type to create counter fields (in counter tables). For
  example:

      @primary_key false
      schema "page_views" do
        field :route, :string, primary_key: true
        field :total, Exandra.Counter
      end

  You can only *update* counter fields. You'll have to use `c:Ecto.Repo.update_all/2`
  to insert or update counters. For example, in the table above, you'd update the
  `:total` counter field with:

      query =
        from page_view in "page_views",
          where: page_view.route == "/browse",
          update: [set: [total: 1]]

      MyApp.Repo.update_all(query)

  ## Batch Queries

  You can run **batch queries** through Exandra. Batch queries are supported by
  Cassandra/Scylla, and allow you to run multiple queries in a single request.
  See `Exandra.Batch` for more information and examples.

  ## Migrations

  You can use Exandra to run migrations as well, as it supports most of the DDL-related
  commands from `Ecto.Migration`. For example:

      defmodule AddUsers do
        use Ecto.Migration

        def change do
          create table("users", primary_key: false) do
            add :email, :string, primary_key: true
            add :age, :int
          end
        end
      end

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
      these get all represented as the `timestamp` type.

  ### User-Defined Types (UDTs)

  `Ecto.Migration` doesn't support creating, altering, or dropping Cassandra/Scylla **UDTs**.
  To do those operations in a migration, use `Ecto.Migration.execute/1`
  or `Ecto.Migration.execute/2`. For example, in your migration module:

      def change do
        execute(
          _up_query = "CREATE TYPE full_name (first_name text, last_name text))",
          _down_query = "DROP TYPE full_name"
        )
      end

  """

  # This overrides the checkout/3 function defined in Ecto.Adapters.SQL. That function
  # is private API, and is defined by "use Ecto.Adapters.SQL". This is why this function
  # needs to appear BEFORE the call to "use Ecto.Adapters.SQL", so that we essentially
  # override the call. We'll work with the Ecto team to make that callback overridable.
  @doc false
  @spec checkout(map(), keyword(), (pid() -> result)) :: result when result: var
  def checkout(%{sql: conn_mod, pid: cluster} = meta, opts, fun) when is_function(fun, 1) do
    conn_mod.checkout(cluster, fun, Keyword.merge(opts, meta.opts))
  end

  use Ecto.Adapters.SQL, driver: :exandra

  @behaviour Ecto.Adapter.Storage

  @xandra_mod Application.compile_env(:exandra, :xandra_module, Xandra)

  @xandra_cluster_mod Application.compile_env(
                        :exandra,
                        :xandra_cluster_module,
                        Xandra.Cluster
                      )

  defmacro embedded_type(field, embedded_schema, opts \\ []) do
    quote do
      opts = Keyword.merge([using: unquote(embedded_schema)], unquote(opts))
      field unquote(field), Exandra.EmbeddedType, opts
    end
  end

  @doc """
  Executes a **batch query**.

  See `Exandra.Batch`.

  ## Examples

      queries = [
        {"INSERT INTO users (email) VALUES (?)", ["jeff@example.com"]},
        {"INSERT INTO users (email) VALUES (?)", ["britta@example.com"]}
      ]

      Exandra.execute_batch(MyApp.Repo, %Exandra.Batch{queries: queries})

  """
  @spec execute_batch(Ecto.Repo.t(), Exandra.Batch.t(), keyword()) ::
          :ok | {:error, Exception.t()}
  def execute_batch(repo, %Exandra.Batch{queries: queries} = _batch, options \\ [])
      when is_atom(repo) and is_list(options) do
    fun = fn conn ->
      try do
        # First, prepare all queries (doesn't matter the order).
        prepared_queries =
          queries
          |> Enum.uniq_by(fn {sql, _values} -> sql end)
          |> Enum.reduce(%{}, fn {sql, _values}, acc ->
            case @xandra_mod.prepare(conn, sql, options) do
              {:ok, %Xandra.Prepared{} = prepared} -> Map.put(acc, sql, prepared)
              {:error, reason} -> throw({:prepare_error, reason})
            end
          end)

        # Now, add them all to a Xandra.Batch and execute the batch.
        batch =
          Enum.reduce(queries, Xandra.Batch.new(), fn {sql, values}, batch ->
            Xandra.Batch.add(batch, Map.fetch!(prepared_queries, sql), values)
          end)

        case @xandra_mod.execute(conn, batch) do
          {:ok, %Xandra.Void{}} -> :ok
          {:error, reason} -> {:errror, reason}
        end
      catch
        {:prepare_error, reason} -> {:error, reason}
      end
    end

    repo.checkout(fun, options)
  end

  @doc false
  def autogenerate(Ecto.UUID), do: Ecto.UUID.generate()
  def autogenerate(:binary_id), do: Ecto.UUID.bingenerate()
  def autogenerate(type), do: super(type)

  @doc false
  @impl Ecto.Adapter
  def dumpers(:binary_id, type), do: [type, Ecto.UUID]
  def dumpers(:map, _type), do: [&Jason.encode/1]
  def dumpers(:naive_datetime, _type), do: [&naive_datetime_to_datetime/1]
  def dumpers({:map, _}, type), do: [&Ecto.Type.embedded_dump(type, &1, :json)]

  def dumpers(:exandra_embedded_type, type) do
    [&Ecto.Type.embedded_dump(type, &1, :exandra_ebedded_type)]
  end

  def dumpers(_, type), do: [type]

  defp naive_datetime_to_datetime(%NaiveDateTime{} = datetime) do
    case DateTime.from_naive(datetime, "Etc/UTC") do
      {:ok, datetime} -> {:ok, datetime}
      {:ambiguous, _first, _second} -> {:error, :ambiguous}
      {:gap, _, _} -> {:error, :gap}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc false
  @impl Ecto.Adapter
  def loaders({:map, _}, type), do: [&Ecto.Type.embedded_load(type, &1, :json)]
  def loaders(:binary_id, _type), do: []
  def loaders(:exandra_map, type), do: [&Ecto.Type.embedded_load(type, &1, :exandra_map), type]
  def loaders(:exandra_set, type), do: [&Ecto.Type.embedded_load(type, &1, :exandra_set), type]

  def loaders(:exandra_embedded, type),
    do: [&Ecto.Type.embedded_load(type, &1, :exandra_ebedded_type), type]

  def loaders(:map, type), do: [&Ecto.Type.load(type, Jason.decode!(&1 || "null"))]
  # Xandra returns UUIDs as strings, so we don't need to do any loading.
  def loaders(:uuid, _type), do: []
  def loaders(:decimal, type), do: [&load_decimal/1, type]
  def loaders(_, type), do: [type]

  defp load_decimal({coefficient, exponent}), do: {:ok, Decimal.new(1, coefficient, -exponent)}

  @impl Ecto.Adapter.Migration
  def lock_for_migrations(_, _, fun), do: fun.()

  @impl Ecto.Adapter.Storage
  def storage_up(opts) do
    {keyspace, conn} = start_storage_connection(opts)

    # https://university.scylladb.com/courses/scylla-essentials-overview/lessons/high-availability/topic/fault-tolerance-replication-factor/
    stmt = """
    CREATE KEYSPACE IF NOT EXISTS #{keyspace}
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'}
    AND durable_writes = true;
    """

    storage_toggle(conn, stmt, "CREATED", :already_up)
  end

  @impl Ecto.Adapter.Storage
  def storage_down(opts) do
    {keyspace, conn} = start_storage_connection(opts)

    storage_toggle(conn, "DROP KEYSPACE IF EXISTS #{keyspace}", "DROPPED", :already_down)
  end

  defp storage_toggle(conn, stmt, effect, error_msg) do
    case @xandra_mod.execute(conn, stmt) do
      {:ok, %Xandra.SchemaChange{effect: ^effect}} ->
        :ok

      {:ok, %Xandra.Void{}} ->
        {:error, error_msg}

      err ->
        err
    end
  end

  @impl Ecto.Adapter.Storage
  def storage_status(opts) do
    {keyspace, conn} = start_storage_connection(opts)

    stmt = "USE KEYSPACE #{keyspace}"

    case @xandra_mod.execute(conn, stmt) do
      {:error, %Xandra.Error{reason: :invalid}} ->
        :down

      {:error, _reason} = err ->
        err

      _ ->
        :up
    end
  end

  @impl Ecto.Adapter.Migration
  def supports_ddl_transaction?, do: false

  defp start_storage_connection(opts) do
    keyspace = Keyword.fetch!(opts, :keyspace)
    Application.ensure_all_started(:exandra)

    {:ok, conn} =
      @xandra_mod.start_link(Keyword.take(opts, [:nodes, :protocol_version, :timeout]))

    {keyspace, conn}
  end

  @doc """
  Streams the results of a simple query or a prepared query.

  See `Xandra.prepare!/4` and `Xandra.stream_pages!/4` for more information including
  supported options.

  ## Examples

      stream =
        Exandra.stream!(
          "SELECT * FROM USERS WHERE can_contact = ?",
          [true],
          MyRepo,
        )

      Enum.each(
        stream,
        fn page ->
          Enum.each(page, fn user -> send_email(user.email, "Hello!") end)
        end
      )
  """
  @spec stream!(Ecto.Repo.t(), String.t(), list(term()), Keyword.t()) :: Xandra.PageStream.t()
  def stream!(repo, sql, values, opts \\ []) do
    %{pid: cluster_pid} = Ecto.Repo.Registry.lookup(repo.get_dynamic_repo())
    prepared = @xandra_cluster_mod.prepare!(cluster_pid, sql, opts)

    @xandra_cluster_mod.stream_pages!(
      cluster_pid,
      prepared,
      values,
      opts
    )
  end
end

defimpl String.Chars, for: [Xandra.Simple, Xandra.Prepared, Xandra.Batch] do
  def to_string(simple) do
    inspect(simple)
  end
end
