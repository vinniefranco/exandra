defmodule Exandra.XMap do
  opts_schema = [
    key: [
      type: :atom,
      required: true,
      doc: "The type of the keys in the map."
    ],
    value: [
      type: :atom,
      required: true,
      doc: "The type of the values in the map."
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

  @moduledoc """
  `Ecto.Type` for maps.

  ## Options

  #{NimbleOptions.docs(opts_schema)}

  ## Examples

      schema "user_metadata" do
        field :free_form_meta, Exandra.XMap, key: :string, value: :string
      end

  """

  use Ecto.ParameterizedType

  alias Exandra.Types

  @opts_schema NimbleOptions.new!(opts_schema)

  # Made public for testing.
  @doc false
  def params(embed), do: %{embed: embed}

  @impl Ecto.ParameterizedType
  def init(opts) do
    opts
    |> NimbleOptions.validate!(@opts_schema)
    |> Map.new()
  end

  @impl Ecto.ParameterizedType
  def type(_params) do
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

  # From Ecto.Type.
  @doc false
  def embed_as(_format), do: :self
end
