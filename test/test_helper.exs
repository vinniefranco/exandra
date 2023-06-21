defmodule Exandra.TestRepo do
  use Ecto.Repo, otp_app: :exandra, adapter: Exandra
end

Mox.defmock(XandraClusterMock, for: Exandra.XandraCluster.Behaviour)
Mox.defmock(XandraMock, for: Exandra.Xandra.Behaviour)

Application.put_env(:exandra, TestRepo,
  default_consistency: :one,
  keyspace: "test_keyspace",
  log_level: :debug,
  migration_primary_key: [name: :id, type: :binary_id],
  primary_key: [name: :id, type: :binary_id],
  nodes: ["test_node"],
  pool_size: 10,
  protocol_version: :v4
)

defmodule Exandra.AdapterCase do
  use ExUnit.CaseTemplate

  import Mox

  setup :set_mox_global
  setup :verify_on_exit!

  setup do
    Mox.stub(XandraClusterMock, :child_spec, fn _opts ->
      Supervisor.child_spec({Agent, fn -> :ok end}, [])
    end)

    start_link_supervised!(Exandra.TestRepo)

    :ok
  end
end

ExUnit.start()
