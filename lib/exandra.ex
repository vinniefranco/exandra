defmodule Exandra do
  use Ecto.Adapters.SQL, driver: :exandra
  alias Ecto.Adapters.SQL

  alias Exandra.Adapter
  alias Exandra.Types

  @behaviour Ecto.Adapter.Storage

  @default_opts [
    decimal_format: :decimal,
    uuid_format: :binary
  ]

  @impl Ecto.Adapter
  def loaders({:map, _}, type),
    do: [&Ecto.Type.embedded_load(type, Jason.decode!(&1 || "null"), :json)]

  def loaders(_key, type), do: [type]

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

  @impl Ecto.Adapter.Schema
  def insert(
        adapter_meta,
        %{source: source, prefix: prefix, schema: schema},
        params,
        {kind, conflict_params, _} = on_conflict,
        returning,
        opts
      ) do
    {fields, _} = :lists.unzip(params)
    # We have to massage some values for Cassandra/Scylla
    prepared_values = prepare_values(schema, params)
    {_, values} = Enum.unzip(prepared_values)
    sql = @conn.insert(prefix, source, fields, [fields], on_conflict, returning, opts)

    opts =
      opts
      |> put_source(source)
      |> Keyword.merge(@default_opts)
      |> Keyword.put(:query, sql)
      |> Keyword.put(:params, prepared_values)

    SQL.struct(
      adapter_meta,
      @conn,
      sql,
      :insert,
      source,
      [],
      values ++ conflict_params,
      kind,
      returning,
      opts
    )
  end

  defp start_storage_connection(opts) do
    keyspace = Keyword.fetch!(opts, :keyspace)
    Application.ensure_all_started(:exandra)

    {:ok, conn} = Adapter.start_link(Keyword.take(opts, [:nodes, :protocol_version, :timeout]))

    {keyspace, conn}
  end

  def put_source(opts, source) when is_binary(source), do: Keyword.put(opts, :source, source)
  def put_source(opts, _), do: opts

  defp prepare_values(schema, params) do
    for source <- Keyword.keys(params) do
      field = source_field(schema, source)
      ecto_type = schema.__schema__(:type, field)
      {source, {ecto_type |> Types.for() |> to_string(), source_value(ecto_type, params[source])}}
    end
  end

  defp source_field(schema, source) do
    :fields
    |> schema.__schema__()
    |> Enum.find(fn
      ^source -> true
      field -> schema.__schema__(:field_source, field) == source
    end)
  end

  # encode values into map if it embeds_many with unique primary_keys
  defp source_value(
         {:parameterized, Ecto.Embedded,
          %Ecto.Embedded{cardinality: :many, unique: true, ordered: ordered, related: schema}},
         values
       )
       when is_list(values) do
    primary_keys =
      :primay_key
      |> schema.__schema__()
      |> Enum.map(&schema.__schema__(:field_source, &1))

    if primary_keys == [] do
      values =
        Enum.map(values, fn value ->
          value
          |> Enum.reject(&match?({_, nil}, &1))
          |> Enum.into(%{})
        end)

      Jason.encode!(values)
    else
      data =
        values
        |> Enum.with_index()
        |> Enum.reduce(%{}, fn {value, index}, acc ->
          key = Enum.map_join(primary_keys, "/", &Map.get(value, &1))
          value = if ordered, do: Map.put(value, :__index__, index), else: value

          value =
            value
            |> Enum.reject(&match?({_, nil}, &1))
            |> Enum.into(%{})

          Map.put(acc, key, value)
        end)

      Jason.encode!(data)
    end
  end

  defp source_value({:parameterized, Ecto.Embedded, _}, %{} = value) do
    value
    |> Enum.reject(&match?({_, nil}, &1))
    |> Enum.into(%{})
    |> Jason.encode!()
  end

  defp source_value({:parameterized, Ecto.Embedded, _}, value), do: Jason.encode!(value)
  defp source_value(_, {:add, value}), do: value
  defp source_value(_, {:remove, value}), do: value
  defp source_value(_, %NaiveDateTime{} = value), do: DateTime.from_naive!(value, "Etc/UTC")
  defp source_value(_, value), do: value
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
