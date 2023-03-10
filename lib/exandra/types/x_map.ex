defmodule Exandra.Types.XMap do
  use Ecto.ParameterizedType

  alias Exandra.Types

  def params(embed), do: %{embed: embed}

  @impl Ecto.ParameterizedType
  def init(opts) do
    Enum.into(opts, %{})
  end

  @impl Ecto.ParameterizedType
  def type(_) do
    :x_map
  end

  @impl Ecto.ParameterizedType
  def cast(nil, _), do: {:ok, %{}}

  def cast({op, %{} = map}, opts) when op in [:add, :remove] do
    case cast(map, opts) do
      {:ok, casted} -> {:ok, {op, casted}}
      other -> other
    end
  end

  def cast(%{} = map, %{key: key_type, value: value_type} = opts) do
    casted =
      Enum.reduce_while(map, %{}, fn {k, v}, acc ->
        with {:ok, casted_key} <- Types.apply(key_type, :cast, k, opts),
             {:ok, casted_value} <- Types.apply(value_type, :cast, v, opts) do
          {:cont, Map.put(acc, casted_key, casted_value)}
        else
          _ -> {:halt, :error}
        end
      end)

    if is_map(casted), do: {:ok, casted}, else: casted
  end

  def cast(_, _), do: :error

  @impl Ecto.ParameterizedType
  def load(%{} = map, _loader, %{key: key_type, value: value_type} = opts) do
    loaded =
      Enum.reduce_while(map, %{}, fn {k, v}, acc ->
        with {:ok, loaded_key} <- Types.apply(key_type, :load, k, opts),
             {:ok, loaded_value} <- Types.apply(value_type, :load, v, opts) do
          {:cont, Map.put(acc, loaded_key, loaded_value)}
        else
          _ -> {:halt, :error}
        end
      end)

    if is_map(loaded), do: {:ok, loaded}, else: :error
  end

  def load(nil, _, _), do: {:ok, %{}}

  def load(_field_name, loader, field) do
    load(%{}, loader, field)
  end

  @impl Ecto.ParameterizedType
  def dump(map, _dumper, _opts), do: {:ok, map}

  @impl Ecto.ParameterizedType
  def equal?({_, _}, _, _), do: false
  def equal?(_, {_, _}, _), do: false
  def equal?(nil, nil, _), do: true
  def equal?(nil, data, _), do: Enum.empty?(data)
  def equal?(data, nil, _), do: Enum.empty?(data)
  def equal?(%{} = a, %{} = b, _), do: Map.equal?(a, b)
  def equal?(_, _, _), do: false

  def embed_as(_), do: :self

  def xandra_type(%{key: key_type, value: value_type}) do
    "map<#{Types.for(key_type)}, #{Types.for(value_type)}>"
  end
end
