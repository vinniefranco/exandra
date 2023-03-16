defmodule Exandra.Types.XCounter do
  use Ecto.Type

  def type, do: :counter

  def cast(val) do
    {:ok, {"counter", val}}
  end

  def load(val) do
    {:ok, val}
  end

  def dump(val) do
    {:ok, val}
  end
end
