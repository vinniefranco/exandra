defmodule Exandra.XCounter do
  @moduledoc """
  `Ecto.Type` for counters.

  ## Examples

    schema "page_views" do
      field :url, :string
      field :views, Exandra.XCounter
    end

  """

  use Ecto.Type

  @impl Ecto.Type
  def type, do: :counter

  @impl Ecto.Type
  def cast(val) when is_integer(val) do
    {:ok, val}
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
