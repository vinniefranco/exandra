defmodule Exandra do
  use Ecto.Adapters.SQL, driver: :exandra

  alias Exandra.Adapter
  alias Exandra.Types

  @behaviour Ecto.Adapter.Storage

  def autogenerate(:binary_id), do: {"uuid", Ecto.UUID.bingenerate()}

  @impl Ecto.Adapter
  def dumpers({:map, _}, type), do: [&Ecto.Type.embedded_dump(type, &1, :json)]
  def dumpers(:binary_id, _type), do: [&encode_uuid/1]
  def dumpers(:map, type), do: [type, Types.XJson]
  def dumpers(_, type), do: [type]

  def encode_uuid(uuid) do
    {:ok, {"uuid", uuid}}
  end

  @impl Ecto.Adapter
  def loaders({:map, _}, type),
    do: [&Ecto.Type.embedded_load(type, Jason.decode!(&1 || "null"), :json)]

  def loaders(:binary_id, type), do: [&decode_binary_id/1, type]

  def loaders(:x_map, type), do: [&Ecto.Type.embedded_load(type, &1, :x_map), type]
  def loaders(:x_set, type), do: [&Ecto.Type.embedded_load(type, &1, :x_set), type]
  def loaders(:x_list, type), do: [&Ecto.Type.embedded_load(type, &1, :x_list), type]
  def loaders(_, type), do: [type]

  # Catch strings
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

    storage_toggle(conn, "DROP KEYSPACE IF EXISTS #{keyspace};", "DROPPED", :already_down)
  end

  defp storage_toggle(conn, stmt, effect, error_msg) do
    case Adapter.execute(conn, stmt) do
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

    stmt = "USE KEYSPACE #{keyspace};"

    case Adapter.execute(conn, stmt) do
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

    {:ok, conn} = Adapter.start_link(Keyword.take(opts, [:nodes, :protocol_version, :timeout]))

    {keyspace, conn}
  end

  def put_source(opts, source) when is_binary(source), do: Keyword.put(opts, :source, source)
  def put_source(opts, _), do: opts
end

defimpl String.Chars, for: Xandra.Simple do
  def to_string(simple) do
    inspect(simple)
  end
end

defimpl String.Chars, for: Xandra.Prepared do
  def to_string(prepared) do
    inspect(prepared)
  end
end

defimpl String.Chars, for: Xandra.Batch do
  def to_string(prepared) do
    inspect(prepared)
  end
end
