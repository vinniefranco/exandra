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

  ## Examples

  To configure your Ecto repository to use this adapter, you can use the
  `:adapter` option. For example, when defining the repo:

      defmodule MyApp.Repo do
        use Ecto.Repo, otp_app: :my_app, adapter: Exandra
      end

  """

  use Ecto.Adapters.SQL, driver: :exandra

  @behaviour Ecto.Adapter.Storage

  @xandra_mod Application.compile_env(:exandra, :xandra_module, Xandra)

  @doc false
  def autogenerate(:binary_id), do: Ecto.UUID.bingenerate()
  def autogenerate(type), do: super(type)

  @doc false
  @impl Ecto.Adapter
  def dumpers(:binary_id, type), do: [type, Ecto.UUID]
  def dumpers(:map, _type), do: [&Jason.encode/1]
  def dumpers(:naive_datetime, _type), do: [&naive_datetime_to_datetime/1]
  def dumpers({:map, _}, type), do: [type]
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
  def loaders({:map, _}, type),
    do: [&Ecto.Type.embedded_load(type, Jason.decode!(&1 || "null"), :json)]

  def loaders(:binary_id, type), do: [Ecto.UUID, type]
  def loaders(:x_map, type), do: [&Ecto.Type.embedded_load(type, &1, :x_map), type]
  def loaders(:x_set, type), do: [&Ecto.Type.embedded_load(type, &1, :x_set), type]
  def loaders(:x_list, type), do: [&Ecto.Type.embedded_load(type, &1, :x_list), type]
  def loaders(:map, type), do: [&Ecto.Type.load(type, Jason.decode!(&1 || "null"))]
  # Xandra returns UUIDs as strings, so we don't need to do any loading.
  def loaders(:uuid, _type), do: []
  def loaders(:decimal, type), do: [&load_decimal/1, type]
  def loaders(_, type), do: [type]

  defp load_decimal({coefficient, exponent}), do: {:ok, Decimal.new(1, coefficient, -exponent)}

  # Catch strings
  @doc false
  def decode_binary_id(<<_::64, ?-, _::32, ?-, _::32, ?-, _::32, ?-, _::96>> = string) do
    {:ok, string}
  end

  def decode_binary_id({"uuid", binuuid}) do
    {:ok, Ecto.UUID.load!(binuuid)}
  end

  def decode_binary_id(id) do
    {:ok, Ecto.UUID.load!(id)}
  end

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
end

defimpl String.Chars, for: [Xandra.Simple, Xandra.Prepared, Xandra.Batch] do
  def to_string(simple) do
    inspect(simple)
  end
end
