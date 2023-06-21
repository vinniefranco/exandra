defmodule Exandra.XandraClusterBehaviour do
  @moduledoc false
  # Module used as a behaviour for Xandra.Cluster, to use as mocks in tests.

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
