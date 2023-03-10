defmodule Exandra.Table do
  defmacro __using__(_opts \\ []) do
    quote do
      use Ecto.Schema

      alias Ecto.UUID
      alias Exandra.Types.XJson
      alias Exandra.Types.XMap
      alias Exandra.Types.XSet
      alias Exandra.Types.XTimestamp

      import Ecto.Changeset
    end
  end
end
