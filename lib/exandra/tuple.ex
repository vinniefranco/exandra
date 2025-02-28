defmodule Exandra.Tuple do
  opts_schema = [
    types: [
      type: {:list, :any},
      required: true,
      doc: "The types of the elements in the tuple."
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
  `Ecto.ParameterizedType` for tuples.

  *Available since v0.11.0*.

  ## Options

  #{NimbleOptions.docs(opts_schema)}

  ## Examples

      schema "user" do
        field :favorite_movie_with_score, Exandra.Tuple, types: [:string, :integer]
      end

  """

  @moduledoc since: "0.11.0"

  use Ecto.ParameterizedType

  alias Exandra.Types

  @type t() :: Tuple.t()

  @opts_schema NimbleOptions.new!(opts_schema)

  # Made public for testing.
  @doc false
  def params(embed), do: %{embed: embed}

  @impl Ecto.ParameterizedType
  def type(_opts), do: :exandra_tuple

  @impl Ecto.ParameterizedType
  def init(opts) do
    {types, opts} = Keyword.pop_first(opts, :types)

    if types == [] do
      raise ArgumentError, "CQL tuples must have at least one element, got: []"
    end

    types = types |> Enum.map(&Types.check_type!(__MODULE__, &1, opts))

    opts
    |> Keyword.put(:types, types)
    |> Keyword.take(Keyword.keys(@opts_schema.schema))
    |> NimbleOptions.validate!(@opts_schema)
    |> Map.new()
  end

  @impl Ecto.ParameterizedType
  def cast(tuple, opts) when is_tuple(tuple), do: tuple |> Tuple.to_list() |> cast(opts)

  def cast(list, %{types: types}) when is_list(list) and length(list) == length(types) do
    casted =
      list
      |> Enum.zip(types)
      |> Enum.reduce_while([], fn {element, type}, acc ->
        case Ecto.Type.cast(type, element) do
          {:ok, value} -> {:cont, [value | acc]}
          error -> {:halt, error}
        end
      end)

    if is_list(casted) do
      casted =
        casted
        |> Enum.reverse()
        |> List.to_tuple()

      {:ok, casted}
    else
      casted
    end
  end

  def cast(nil, _), do: {:ok, nil}

  def cast(val, %{types: [type]}) do
    case Ecto.Type.cast(type, val) do
      {:ok, casted} -> {:ok, {casted}}
      err -> err
    end
  end

  def cast(_key, _val), do: :error

  @impl Ecto.ParameterizedType
  def load(value, loader \\ &Ecto.Type.load/2, params)

  def load(tuple, loader, %{types: types}) when is_tuple(tuple) do
    loaded =
      tuple
      |> Tuple.to_list()
      |> Enum.zip(types)
      |> Enum.reduce_while([], fn {element, type}, acc ->
        case Ecto.Type.load(type, element, loader) do
          {:ok, value} -> {:cont, [value | acc]}
          error -> {:halt, error}
        end
      end)

    if is_list(loaded) do
      loaded =
        loaded
        |> Enum.reverse()
        |> List.to_tuple()

      {:ok, loaded}
    else
      loaded
    end
  end

  def load(nil, _, _), do: {:ok, nil}

  def load(_field_name, loader, field) do
    load(nil, loader, field)
  end

  @impl Ecto.ParameterizedType
  def dump(nil, _dumper, _opts), do: {:ok, nil}

  def dump(tuple, dumper, %{types: types}) do
    dumped =
      tuple
      |> Tuple.to_list()
      |> Enum.zip(types)
      |> Enum.reduce_while([], fn {element, type}, acc ->
        case Ecto.Type.dump(type, element, dumper) do
          {:ok, value} -> {:cont, [value | acc]}
          error -> {:halt, error}
        end
      end)

    if is_list(dumped) do
      dumped =
        dumped
        |> Enum.reverse()
        |> List.to_tuple()

      {:ok, dumped}
    else
      dumped
    end
  end

  @doc false
  @impl Ecto.ParameterizedType
  def embed_as(_format, _opts), do: :self
end
