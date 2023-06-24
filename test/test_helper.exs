Mox.defmock(XandraClusterMock, for: Exandra.XandraClusterBehaviour)
Mox.defmock(XandraMock, for: Exandra.XandraBehaviour)

Application.put_env(:exandra, Exandra.TestRepo,
  default_consistency: :one,
  keyspace: "test_keyspace",
  log_level: :debug,
  migration_primary_key: [name: :id, type: :binary_id],
  primary_key: [name: :id, type: :binary_id],
  nodes: ["test_node"],
  pool_size: 10,
  protocol_version: :v4
)

ExUnit.start()
