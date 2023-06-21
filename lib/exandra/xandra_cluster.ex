defmodule Exandra.XandraCluster do
  @moduledoc false
  # Module used as a proxy to Xandra.Cluster, and behaviour to use as mocks in tests.

  defmodule Behaviour do
    @moduledoc false
    alias Xandra.Prepared

    @callback child_spec(keyword()) :: Supervisor.child_spec()

    @callback execute(
                cluster :: pid(),
                Xandra.statement() | Prepared.t(),
                Xandra.values(),
                keyword
              ) ::
                {:ok, Xandra.result()} | {:error, Xandra.error()}

    @callback prepare(cluster :: pid(), stmt :: Xandra.statement(), keyword()) ::
                {:ok, Prepared.t()} | {:error, term()}

    @callback stream_pages!(
                cluster :: pid(),
                stmt :: Xandra.statement() | Prepared.t(),
                values :: Xandra.values(),
                keyword :: keyword()
              ) :: Enumerable.t()
  end

  @behaviour Behaviour

  @impl true
  def child_spec(opts) do
    repo = Keyword.fetch!(opts, :repo)
    keyspace = Keyword.fetch!(opts, :keyspace)

    opts = Keyword.put(opts, :after_connect, &Xandra.execute!(&1, "USE #{keyspace}"))

    Supervisor.child_spec({Xandra.Cluster, opts}, id: repo)
  end

  @impl true
  defdelegate execute(cluster, sql, params, opts), to: Xandra.Cluster

  @impl true
  defdelegate prepare(cluster, stmt, opts), to: Xandra.Cluster

  @impl true
  defdelegate stream_pages!(cluster, query, params, opts), to: Xandra.Cluster
end
