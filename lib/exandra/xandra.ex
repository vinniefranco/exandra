defmodule Exandra.XandraBehaviour do
  @moduledoc false
  # Module used as behaviour for Xandra (which we use for storage up/storage down functionality),
  # to use with mocks in tests.

  @callback start_link(keyword()) :: GenServer.on_start()

  @callback execute(Xandra.conn(), String.t()) ::
              {:ok, Xandra.result()} | {:error, Xandra.error()}
end
