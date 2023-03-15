defmodule Exandra.Types do
  alias Exandra.Types.XMap
  alias Exandra.Types.XSet
  alias Exandra.Types.UDT

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

  def for(type, opts \\ [])

  def for(:id, _), do: :uuid
  def for(:binary_id, _), do: :uuid
  def for(:integer, _), do: :int
  def for(:string, _), do: :text
  def for(:binary, _), do: :blob
  def for(:map, _), do: :text

  def for(UDT, opts) do
    type = Keyword.get(opts, :type)

    if is_nil(type), do: raise(ArgumentError, "must define :type option for UDT column")

    "FROZEN<#{type}>"
  end

  def for(t, _opts) when t in [:datetime, :naive_datetime, :utc_datetime, :utc_datetime_usec],
    do: :timestamp

  def for({:parameterized, Ecto.Embedded, _}, _opts), do: :text
  def for({:parameterized, Ecto.Enum, _}, _opts), do: :text
  def for({:parameterized, XMap, opts}, _opts), do: XMap.xandra_type(opts)
  def for({:parameterized, XSet, opts}, _opts), do: XSet.xandra_type(opts)

  def for({:set, opts}, _opts), do: XSet.type(opts)

  def for(ecto_type, _opts) do
    if is_atom(ecto_type) and function_exported?(ecto_type, :type, 0) do
      ecto_type.type()
    else
      ecto_type
    end
  end
end
