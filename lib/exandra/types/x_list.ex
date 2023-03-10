defmodule Exandra.Types.XList do
  use Ecto.ParameterizedType

  alias Exandra.Types

  def params(embed), do: %{embed: embed}

  @impl Ecto.ParameterizedType
  def type(_), do: :x_list

  @impl Ecto.ParameterizedType
  def init(opts) do
    Enum.into(opts, %{})
  end

  @impl Ecto.ParameterizedType
  def cast(nil, _), do: {:ok, []}

  def cast({op, val}, opts) when op in [:add, :remove] do
    case cast(val, opts) do
      {:ok, casted} -> {:ok, {op, casted}}
      other -> other
    end
  end

  def cast(list, %{type: type} = opts) when is_list(list) do
    casted =
      Enum.reduce_while(list, [], fn elem, acc ->
        case Types.apply(type, :cast, elem, opts) do
          {:ok, casted} -> {:cont, [casted | acc]}
          err -> {:halt, err}
        end
      end)

    if is_list(casted), do: {:ok, Enum.reverse(casted)}, else: casted
  end

  def cast(val, %{type: type} = opts) do
    case Types.apply(type, :cast, val, opts) do
      {:ok, casted} -> {:ok, [casted]}
      err -> err
    end
  end

  def cast(_, _), do: :error

  @impl Ecto.ParameterizedType
  def load(list, _loader, %{type: type} = opts) when is_list(list) do
    loaded =
      Enum.reduce_while(list, [], fn elem, acc ->
        case Types.apply(type, :load, elem, opts) do
          {:ok, loaded} -> {:cont, [loaded | acc]}
          err -> {:halt, err}
        end
      end)

    if is_list(loaded), do: {:ok, Enum.reverse(loaded)}, else: :error
  end

  def load(nil, _, _), do: {:ok, []}

  def load(_field_name, loader, field) do
    load([], loader, field)
  end

  @impl Ecto.ParameterizedType
  def dump(data, _dumper, opts), do: {:ok, {xandra_type(opts), data}}

  @impl Ecto.ParameterizedType
  def equal?({_, _}, _, _), do: false
  def equal?(_, {_, _}, _), do: false
  def equal?(nil, nil, _), do: true
  def equal?(nil, data, _), do: Enum.empty?(data)
  def equal?(data, nil, _), do: Enum.empty?(data)
  def equal?(a, b, _) when is_list(a) and is_list(b), do: a == b
  def equal?(_, _, _), do: false

  def embed_as(_), do: :self

  def xandra_type(%{type: type}), do: "list<#{Types.for(type)}>"
end
