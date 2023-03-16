defmodule Exandra.Table do
  defmacro __using__(_opts \\ []) do
    quote do
      use Ecto.Schema

      alias Ecto.UUID
      alias Exandra.Types.UDT
      alias Exandra.Types.XCounter
      alias Exandra.Types.XMap
      alias Exandra.Types.XSet

      import Ecto.Changeset
    end
  end
end
