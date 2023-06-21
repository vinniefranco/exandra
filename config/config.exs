import Config

if config_env() == :test do
  config :exandra,
    xandra_cluster_module: XandraClusterMock,
    xandra_module: XandraMock

  config :logger,
    level: :info
end
