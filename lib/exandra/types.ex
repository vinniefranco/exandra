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

  def for({:array, UDT}, opts) do
    with {:ok, subtype} <- __MODULE__.for(UDT, opts), do: {:ok, "FROZEN<list<#{subtype}>>"}
  end

  def for({:array, subtype}, _opts) do
    with {:ok, subtype} <- __MODULE__.for(subtype), do: {:ok, "list<#{subtype}>"}
  end

  def for({:parameterized, Ecto.Embedded, _}, _opts), do: {:ok, "text"}
  def for({:parameterized, Ecto.Enum, _}, _opts), do: {:ok, "text"}

  def for(ecto_type, _opts) when is_atom(ecto_type) do
    if Code.ensure_loaded?(ecto_type) and function_exported?(ecto_type, :type, 0) do
      {:ok, to_string(ecto_type.type())}
    else
      {:ok, Atom.to_string(ecto_type)}
    end
  end

  def for(_ecto_type, _opts), do: :error

  @spec check_type!(module(), any(), keyword()) :: Ecto.Type.t()
  def check_type!(name, type, opts) when is_atom(type) do
    cond do
      Ecto.Type.base?(type) -> type
      Code.ensure_compiled(type) == {:module, type} -> check_parameterized(name, type, opts)
      true -> raise ArgumentError, "#{name}: not a valid type parameter, got #{inspect(type)}"
    end
  end

  def check_type!(name, {composite, inner}, opts) do
    if Ecto.Type.composite?(composite) do
      inner = check_type!(name, inner, opts)
      {composite, inner}
    else
      raise ArgumentError, "#{name}: expected Ecto composite type, got: #{inspect(composite)}"
    end
  end

  def check_type!(name, any, _opts),
    do: raise(ArgumentError, "#{name}: unknown type parameter, got: #{inspect(any)}")

  defp check_parameterized(name, type, opts) do
    cond do
      function_exported?(type, :type, 0) ->
        type

      function_exported?(type, :type, 1) ->
        Ecto.ParameterizedType.init(type, opts)

      true ->
        raise ArgumentError,
              "#{name}: expected Ecto.Type/Ecto.ParameterizedType, got: #{inspect(type)}"
    end
  end
end
