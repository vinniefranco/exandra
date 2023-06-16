defmodule Exandra.Types.XSet do
  @moduledoc """
  `Ecto.Type` for sets.
  """

  use Ecto.ParameterizedType

  alias Exandra.Types

  @opts_schema [
    type: [
      type: :atom,
      required: true,
      doc: "The type of the elements in the set."
    ],
    field: [
      type: :atom,
      doc: false
    ],
    schema: [
      type: :atom,
      doc: false
    ]
  ]

  # Made public for testing.
  @doc false
  def params(embed), do: %{embed: embed}

  @impl Ecto.ParameterizedType
  def type(_), do: :x_set

  @impl Ecto.ParameterizedType
  def init(opts) do
    opts
    |> NimbleOptions.validate!(@opts_schema)
    |> Map.new()
  end

  @impl Ecto.ParameterizedType
  def cast(nil, _), do: {:ok, MapSet.new()}

  def cast({op, val}, opts) when op in [:add, :remove] do
    case cast(val, opts) do
      {:ok, casted} -> {:ok, {op, casted}}
      other -> other
    end
  end

  def cast(%MapSet{} = set, opts), do: set |> MapSet.to_list() |> cast(opts)

  def cast(list, %{type: type} = opts) when is_list(list) do
    casted =
      Enum.reduce_while(list, [], fn elem, acc ->
        case Types.apply(type, :cast, elem, opts) do
          {:ok, casted} -> {:cont, [casted | acc]}
          err -> {:halt, err}
        end
      end)

    if is_list(casted), do: {:ok, MapSet.new(casted)}, else: casted
  end

  def cast(val, %{type: type} = opts) do
    case Types.apply(type, :cast, val, opts) do
      {:ok, casted} -> {:ok, MapSet.new([casted])}
      err -> err
    end
  end

  def cast(_key, _val), do: :error

  @impl Ecto.ParameterizedType
  def load(%MapSet{} = mapset, _loader, %{type: type} = opts) do
    loaded =
      Enum.reduce_while(mapset, [], fn elem, acc ->
        case Types.apply(type, :load, elem, opts) do
          {:ok, loaded} -> {:cont, [loaded | acc]}
          err -> {:halt, err}
        end
      end)

    if is_list(loaded), do: {:ok, MapSet.new(loaded)}, else: :error
  end

  def load(nil, _, _), do: {:ok, %MapSet{}}

  def load(_field_name, loader, field) do
    load(%MapSet{}, loader, field)
  end

  @impl Ecto.ParameterizedType
  def dump(mapset, _dumper, opts), do: {:ok, {xandra_type(opts), mapset}}

  @impl Ecto.ParameterizedType
  def equal?({_, _}, _, _), do: false
  def equal?(_, {_, _}, _), do: false
  def equal?(nil, nil, _), do: true
  def equal?(nil, data, _), do: Enum.empty?(data)
  def equal?(data, nil, _), do: Enum.empty?(data)
  def equal?(%MapSet{} = a, %MapSet{} = b, _), do: MapSet.equal?(a, b)
  def equal?(_, _, _), do: false

  # From Ecto.Type
  @doc false
  def embed_as(_format), do: :self

  @doc false
  def xandra_type(%{type: type}), do: "set<#{Types.for(type)}>"

  defimpl Jason.Encoder, for: MapSet do
    def encode(set, opts) do
      Jason.Encode.list(MapSet.to_list(set), opts)
    end
  end
end
