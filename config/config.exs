import Config

if config_env() == :test do
  config :exandra,
    xandra_cluster_module: XandraClusterMock,
    xandra_module: XandraMock

  config :logger,
    level: String.to_existing_atom(System.get_env("LOG_LEVEL", "info"))
end
