defmodule Exandra.Set do
  opts_schema = [
    type: [
      type: :any,
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

  @moduledoc """
  `Ecto.ParameterizedType` for sets.

  ## Options

  #{NimbleOptions.docs(opts_schema)}

  ## Examples

      schema "users" do
        field :email, :string
        field :roles, Exandra.Set, type: :string
      end

  """

  use Ecto.ParameterizedType

  alias Exandra.Types

  @type t() :: MapSet.t()

  @opts_schema NimbleOptions.new!(opts_schema)

  # Made public for testing.
  @doc false
  def params(embed), do: %{embed: embed}

  @impl Ecto.ParameterizedType
  def type(_opts), do: :exandra_set

  @impl Ecto.ParameterizedType
  def init(opts) do
    {type, opts} = Keyword.pop_first(opts, :type)
    checked_type = Types.check_type!(__MODULE__, type, opts)

    opts
    |> Keyword.put(:type, checked_type)
    |> Keyword.take(Keyword.keys(@opts_schema.schema))
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

  def cast(list, %{type: type}) when is_list(list) do
    casted =
      Enum.reduce_while(list, [], fn elem, acc ->
        case Ecto.Type.cast(type, elem) do
          {:ok, casted} -> {:cont, [casted | acc]}
          err -> {:halt, err}
        end
      end)

    if is_list(casted), do: {:ok, MapSet.new(casted)}, else: casted
  end

  def cast(val, %{type: type}) do
    case Ecto.Type.cast(type, val) do
      {:ok, casted} -> {:ok, MapSet.new([casted])}
      err -> err
    end
  end

  def cast(_key, _val), do: :error

  @impl Ecto.ParameterizedType
  def load(%MapSet{} = mapset, loader, %{type: type}) do
    loaded =
      Enum.reduce_while(mapset, [], fn elem, acc ->
        case Ecto.Type.load(type, elem, loader) do
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
  def dump(nil, _dumper, _params), do: {:ok, nil}

  def dump(mapset, dumper, %{type: type}) do
    dumped =
      Enum.reduce_while(mapset, [], fn elem, acc ->
        case Ecto.Type.dump(type, elem, dumper) do
          {:ok, dumped} -> {:cont, [dumped | acc]}
          err -> {:halt, err}
        end
      end)

    if is_list(dumped), do: {:ok, MapSet.new(dumped)}, else: :error
  end

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

  defimpl Jason.Encoder, for: MapSet do
    def encode(set, opts) do
      Jason.Encode.list(MapSet.to_list(set), opts)
    end
  end
end
