defmodule Exandra.Types do
  @moduledoc false

  alias Exandra.UDT

  @timestamp_types [
    :datetime,
    :naive_datetime,
    :naive_datetime_usec,
    :utc_datetime,
    :utc_datetime_usec
  ]

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

  @spec for(Ecto.Type.t()) :: {:ok, String.t()} | :error
  def for(type, opts \\ [])

  def for(:id, _), do: {:ok, "uuid"}
  def for(:binary_id, _), do: {:ok, "uuid"}
  def for(:integer, _), do: {:ok, "int"}
  def for(:string, _), do: {:ok, "text"}
  def for(:binary, _), do: {:ok, "blob"}
  def for(:map, _), do: {:ok, "text"}
  def for(UDT, opts), do: {:ok, "FROZEN<#{UDT.__validate__(opts)[:type]}>"}
  def for(type, _opts) when type in @timestamp_types, do: {:ok, "timestamp"}

  def for({:array, subtype}, _opts) do
    with {:ok, subtype} <- __MODULE__.for(subtype), do: {:ok, "list<#{subtype}>"}
  end

  def for({:parameterized, Ecto.Embedded, _}, _opts), do: {:ok, "text"}
  def for({:parameterized, Ecto.Enum, _}, _opts), do: {:ok, "text"}

  def for(ecto_type, _opts) when is_atom(ecto_type) do
    if function_exported?(ecto_type, :type, 0) do
      {:ok, ecto_type.type()}
    else
      {:ok, Atom.to_string(ecto_type)}
    end
  end

  def for(_ecto_type, _opts), do: :error
end
