defmodule Exandra.Xandra do
  @moduledoc false
  # Module used as a proxy to Xandra (which we use for storage up/storage down functionality),
  # and behaviour to use as mocks in tests.

  defmodule Behaviour do
    @moduledoc false

    @callback start_link(keyword()) :: GenServer.on_start()

    @callback execute(Xandra.conn(), String.t()) ::
                {:ok, Xandra.result()} | {:error, Xandra.error()}
  end

  @behaviour Behaviour

  @impl true
  defdelegate start_link(opts), to: Xandra

  @impl true
  defdelegate execute(conn, stmt), to: Xandra
end
