defmodule Exandra.AdapterCase do
  use ExUnit.CaseTemplate

  import Mox

  @port String.to_integer(System.get_env("EXANDRA_PORT", "9042"))

  using options do
    quote do
      import unquote(__MODULE__)

      @port unquote(@port)
      @repo_app :exandra

      if unquote(options[:integration]) do
        @moduletag :integration?
      end
    end
  end

  setup :set_mox_from_context
  setup :verify_on_exit!
  setup :generate_unique_keyspace

  setup context do
    if context[:integration?] do
      stub_with_real_modules()
    else
      XandraClusterMock
      |> stub(:child_spec, fn _opts -> Supervisor.child_spec({Agent, fn -> :ok end}, []) end)
      |> stub(:run, fn _cluster, fun -> fun.(self()) end)
    end

    :ok
  end

  def stub_with_real_modules(_context \\ %{}) do
    stub(XandraClusterMock, :child_spec, &Xandra.Cluster.child_spec/1)
    stub(XandraClusterMock, :execute, &Xandra.Cluster.execute/4)
    stub(XandraClusterMock, :prepare, &Xandra.Cluster.prepare/3)
    stub(XandraClusterMock, :stream_pages!, &Xandra.Cluster.stream_pages!/4)
    stub(XandraClusterMock, :run, &Xandra.Cluster.run/2)
    stub(XandraClusterMock, :run, &Xandra.Cluster.run/3)

    stub(XandraMock, :start_link, &Xandra.start_link/1)
    stub(XandraMock, :execute, &Xandra.execute/2)
    stub(XandraMock, :execute, &Xandra.execute/4)
    stub(XandraMock, :prepare, &Xandra.prepare/3)

    :ok
  end

  def create_keyspace(keyspace) when is_binary(keyspace) do
    opts = [host: "localhost", port: @port, keyspace: keyspace, sync_connect: 1000]
    assert Exandra.storage_up(opts) in [:ok, {:error, :already_up}]
  end

  def drop_keyspace(keyspace) when is_binary(keyspace) do
    opts = [host: "localhost", port: @port, keyspace: keyspace, sync_connect: 1000]
    assert Exandra.storage_down(opts) in [:ok, {:error, :already_down}]
  end

  def truncate_all_tables(keyspace) do
    {:ok, conn} = Xandra.start_link(host: "localhost", port: @port, sync_connect: 1000)
    Xandra.execute!(conn, "USE #{keyspace}")

    query = "SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?"

    conn
    |> Xandra.execute!(query, [{"varchar", keyspace}])
    |> Enum.each(fn %{"table_name" => table} -> Xandra.execute!(conn, "TRUNCATE #{table}") end)

    Xandra.stop(conn)
  end

  def drop_all_tables(keyspace) do
    {:ok, conn} = Xandra.start_link(host: "localhost", port: @port, sync_connect: 1000)
    Xandra.execute!(conn, "USE #{keyspace}")

    query = "SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?"

    conn
    |> Xandra.execute!(query, [{"varchar", keyspace}])
    |> Enum.each(fn %{"table_name" => table} ->
      Xandra.execute!(conn, "DROP TABLE IF EXISTS #{table}")
    end)

    Xandra.stop(conn)
  end

  defp generate_unique_keyspace(%{test: test_name} = _context) do
    keyspace =
      test_name
      |> Atom.to_string()
      |> String.trim_leading("test ")
      |> String.downcase()
      |> String.replace(~r/([^a-zA-Z0-9_]|\s)/, "_")
      |> then(&"exandra_integration_#{&1}")
      |> String.slice(0..47)

    %{unique_keyspace: keyspace}
  end
end
