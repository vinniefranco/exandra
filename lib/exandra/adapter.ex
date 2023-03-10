defmodule Exandra.Adapter do
  def child_spec(opts) do
    conn_spec().child_spec(opts)
  end

  def prepare(cluster, stmt, opts) do
    adapter().prepare(cluster, stmt, opts)
  end

  def stream_pages!(cluster, query, params, opts) do
    adapter().stream_pages!(cluster, query, params, opts)
  end

  def execute(conn, stmt) do
    adapter().execute(conn, stmt)
  end

  def execute(cluster, sql, params, opts) do
    adapter().execute(cluster, sql, params, opts)
  end

  def start_link(opts) do
    adapter().start_link(opts)
  end

  def adapter do
    Application.get_env(:exandra, :adapter)
  end

  def conn_spec do
    Application.get_env(:exandra, :child_spec) || adapter()
  end
end
