defmodule Exandra.Types do
  alias Exandra.Types.XMap
  alias Exandra.Types.XSet

  def apply(type, op, value, opts) do
    cond do
      is_atom(type) and function_exported?(type, op, 1) ->
        apply(type, op, [value])

      is_atom(type) and function_exported?(type, op, 2) ->
        apply(type, op, [value, opts])

      true ->
        Ecto.Type.cast(type, value)
    end
  end

  def for(:id), do: :uuid
  def for(:binary_id), do: :uuid
  def for(:integer), do: :int
  def for(:string), do: :text
  def for(:binary), do: :blob

  def for(t) when t in [:naive_datetime, :utc_datetime, :utc_datetime_usec],
    do: :timestamp

  def for({:parameterized, Ecto.Embedded, _}), do: :text
  def for({:parameterized, Ecto.Enum, _}), do: :text
  def for({:parameterized, XMap, opts}), do: XMap.xandra_type(opts)
  def for({:parameterized, XSet, opts}), do: XSet.xandra_type(opts)

  def for({:set, opts}), do: XSet.type(opts)

  def for(ecto_type) do
    if is_atom(ecto_type) and function_exported?(ecto_type, :type, 0) do
      ecto_type.type()
    else
      ecto_type
    end
  end
end
