defmodule Exandra.XandraBehaviour do
  @moduledoc false
  # Module used as behaviour for Xandra (which we use for storage up/storage down functionality),
  # to use with mocks in tests.

  @callback start_link(keyword()) :: GenServer.on_start()

  @callback execute(Xandra.conn(), String.t()) ::
              {:ok, Xandra.result()} | {:error, Xandra.error()}

  @callback execute(Xandra.conn(), String.t(), Xandra.values(), keyword()) ::
              {:ok, Xandra.result()} | {:error, Xandra.error()}

  @callback prepare(Xandra.conn(), String.t(), keyword()) ::
              {:ok, Xandra.Prepared.t()} | {:error, Xandra.error()}
end
