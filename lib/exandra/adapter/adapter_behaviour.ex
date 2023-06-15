defmodule Exandra.Adapter.AdapterBehaviour do
  @moduledoc false

  alias Xandra.Prepared

  @callback child_spec(opts :: any()) :: any()

  @callback start_link(opts :: any()) :: GenServer.on_start()

  @callback execute(conn :: pid(), statement :: Xandra.statement() | Prepared.t()) ::
              {:ok, term()} | {:error, term()}

  @callback execute(cluster :: pid(), Xandra.statement() | Prepared.t(), Xandra.values(), keyword) ::
              {:ok, Xandra.result()} | {:error, Xandra.error()}

  @callback prepare(cluster :: pid(), stmt :: Xandra.statement(), keyword) ::
              {:ok, Prepared.t()} | {:error, term()}

  @callback stream_pages!(
              cluster :: pid(),
              stmt :: Xandra.statement() | Prepared.t(),
              values :: Xandra.values(),
              keyword :: keyword()
            ) :: Enumerable.t()
end
