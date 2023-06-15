defmodule Exandra.Adapter.XandraClustered do
  @moduledoc false

  @behaviour Exandra.Adapter.AdapterBehaviour

  def child_spec(opts) do
    repo = Keyword.fetch!(opts, :repo)
    keyspace = Keyword.fetch!(opts, :keyspace)

    opts = Keyword.put(opts, :after_connect, &Xandra.execute!(&1, "USE #{keyspace};"))

    Supervisor.child_spec({Xandra.Cluster, opts}, id: repo)
  end

  def start_link(opts) do
    Xandra.start_link(opts)
  end

  def execute(conn, stmt) do
    Xandra.execute(conn, stmt)
  end

  def execute(cluster, sql, params, opts) do
    Xandra.Cluster.execute(cluster, sql, params, opts)
  end

  def prepare(cluster, stmt, opts) do
    Xandra.Cluster.prepare(cluster, stmt, opts)
  end

  def stream_pages!(cluster, query, params, opts) do
    Xandra.Cluster.stream_pages!(cluster, query, params, opts)
  end
end
