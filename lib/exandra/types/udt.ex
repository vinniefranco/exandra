defmodule Exandra.Types.UDT do
  use Ecto.ParameterizedType

  @impl Ecto.ParameterizedType
  def type(_params), do: :udt

  @impl Ecto.ParameterizedType
  def init(opts) do
    Enum.into(opts, %{})
  end

  @impl Ecto.ParameterizedType
  def cast(data, %{type: udt}) do
    {:ok, {"#{udt}", data}}
  end

  @impl Ecto.ParameterizedType
  def load(data, _loader, _params) do
    {:ok, data}
  end

  @impl Ecto.ParameterizedType
  def dump(data, _dumper, _params) do
    {:ok, data}
  end

  @impl Ecto.ParameterizedType
  def equal?(a, b, _params) do
    a == b
  end
end
