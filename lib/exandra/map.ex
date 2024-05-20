defmodule Exandra.Map do
  opts_schema = [
    key: [
      type: :any,
      required: true,
      doc: "The type of the keys in the map."
    ],
    value: [
      type: :any,
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
  `Ecto.ParameterizedType` for maps.

  ## Options

  #{NimbleOptions.docs(opts_schema)}

  ## Examples

      schema "user_metadata" do
        field :free_form_meta, Exandra.Map, key: :string, value: :string
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
    {key, opts} = Keyword.pop_first(opts, :key)
    {value, opts} = Keyword.pop_first(opts, :value)

    key = Types.check_type!(__MODULE__, key, opts)
    value = Types.check_type!(__MODULE__, value, opts)

    opts
    |> Keyword.put(:key, key)
    |> Keyword.put(:value, value)
    |> Keyword.take(Keyword.keys(@opts_schema.schema))
    |> NimbleOptions.validate!(@opts_schema)
    |> Map.new()
  end

  @impl Ecto.ParameterizedType
  def type(_params), do: :exandra_map

  @impl Ecto.ParameterizedType
  def cast(nil, _), do: {:ok, %{}}

  def cast({op, %{} = map}, opts) when op in [:add, :remove] do
    case cast(map, opts) do
      {:ok, casted} -> {:ok, {op, casted}}
      other -> other
    end
  end

  def cast(%{} = map, %{key: key_type, value: value_type}) do
    casted =
      Enum.reduce_while(map, %{}, fn {k, v}, acc ->
        with {:ok, casted_key} <- Ecto.Type.cast(key_type, k),
             {:ok, casted_value} <- Ecto.Type.cast(value_type, v) do
          {:cont, Map.put(acc, casted_key, casted_value)}
        else
          _ -> {:halt, :error}
        end
      end)

    if is_map(casted), do: {:ok, casted}, else: casted
  end

  def cast(_, _), do: :error

  @impl Ecto.ParameterizedType
  def load(%{} = map, loader, %{key: key_type, value: value_type}) do
    loaded =
      Enum.reduce_while(map, %{}, fn {k, v}, acc ->
        with {:ok, loaded_key} <- Ecto.Type.load(key_type, k, loader),
             {:ok, loaded_value} <- Ecto.Type.load(value_type, v, loader) do
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
  def dump(nil, _dumper, _params), do: {:ok, nil}

  def dump(map, dumper, %{key: key_type, value: value_type}) do
    dumped =
      Enum.reduce_while(map, %{}, fn {k, v}, acc ->
        with {:ok, dumped_key} <- Ecto.Type.dump(key_type, k, dumper),
             {:ok, dumped_value} <- Ecto.Type.dump(value_type, v, dumper) do
          {:cont, Map.put(acc, dumped_key, dumped_value)}
        else
          _ -> {:halt, :error}
        end
      end)

    if is_map(dumped), do: {:ok, dumped}, else: :error
  end

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
