defmodule Exandra.IntegrationTest do
  use ExUnit.Case, async: false

  @moduletag :integration

  import Mox

  @port String.to_integer(System.get_env("EXANDRA_PORT", "9042"))

  defmodule Repo do
    use Ecto.Repo, otp_app: :exandra_integration, adapter: Exandra
  end

  setup :set_mox_global
  setup :verify_on_exit!

  test "end-to-end flow" do
    xandra_conn = start_link_supervised!({Xandra, [port: @port]})

    Xandra.execute!(
      xandra_conn,
      "CREATE KEYSPACE IF NOT EXISTS exandra_integration WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };"
    )

    stub_with(XandraClusterMock, Exandra.XandraCluster)

    Application.put_env(:exandra_integration, Repo,
      keyspace: "exandra_integration",
      hostname: "localhost",
      port: @port,
      sync_connect: 1000
    )

    start_supervised!(Repo)

    result = Repo.query!("SELECT * FROM system_schema.keyspaces")
    assert result.num_rows > 0
  end
end
