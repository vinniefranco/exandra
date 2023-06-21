defmodule Exandra.Table do
  @moduledoc """
  Utilities to define Exandra-based tables.
  """

  defmacro __using__(_opts \\ []) do
    quote do
      use Ecto.Schema

      alias Ecto.UUID
      alias Exandra.UDT
      alias Exandra.XCounter
      alias Exandra.XMap
      alias Exandra.XSet

      import Ecto.Changeset
    end
  end
end
