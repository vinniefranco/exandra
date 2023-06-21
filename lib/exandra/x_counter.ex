defmodule Exandra.XCounter do
  @moduledoc """
  `Ecto.Type` for counters.
  """

  use Ecto.Type

  @impl Ecto.Type
  def type, do: :counter

  @impl Ecto.Type
  def cast(val) do
    {:ok, {"counter", val}}
  end

  @impl Ecto.Type
  def load(val) do
    {:ok, val}
  end

  @impl Ecto.Type
  def dump(val) do
    {:ok, val}
  end
end
