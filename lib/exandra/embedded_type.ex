defmodule Exandra.EmbeddedType do
  @moduledoc false
  use Ecto.ParameterizedType

  defstruct [
    :using,
    :cardinality,
    :field
  ]

  # Made public for testing.
  @doc false
  def params(embed), do: %{embed: embed}

  @impl Ecto.ParameterizedType
  def init(opts) do
    cardinality = Keyword.get(opts, :cardinality, :one)
    opts = Keyword.put(opts, :cardinality, cardinality)
    struct(__MODULE__, opts)
  end

  @impl Ecto.ParameterizedType
  def type(_), do: :exandra_embedded_type

  @impl Ecto.ParameterizedType
  def cast(nil, %{cardinality: :one}) do
    {:ok, nil}
  end

  def cast(nil, %{cardinality: :many}) do
    {:ok, []}
  end

  def cast(data, %{cardinality: :one, using: struct}) do
    struct
    |> struct(%{})
    |> struct.changeset(data)
    |> Ecto.Changeset.apply_action(:insert)
    |> case do
      {:ok, casted} -> {:ok, casted}
      {:error, _changeset} -> :error
    end
  end

  def cast(data, %{cardinality: :many, using: struct}) do
    data
    |> Enum.reduce_while([], fn datum, acc ->
      case cast(datum, %{cardinality: :one, using: struct}) do
        {:ok, casted} -> {:cont, [casted | acc]}
        :error -> {:halt, :error}
      end
    end)
    |> case do
      :error -> :error
      casted_list -> {:ok, Enum.reverse(casted_list)}
    end
  end

  @impl Ecto.ParameterizedType
  def load(nil, _loader, %{cardinality: cardinality}) do
    val =
      if cardinality == :one do
        nil
      else
        []
      end

    {:ok, val}
  end

  def load(value, loader, %{cardinality: :one, using: struct}) do
    {:ok, Ecto.Schema.Loader.unsafe_load(struct, value, loader)}
  end

  def load(_data, _loader, _opts), do: :error

  @impl Ecto.ParameterizedType
  def dump(nil, _dumper, _opts), do: {:ok, nil}

  def dump(%struct{} = data, dumper, %{cardinality: :one, using: struct}) do
    types = struct.__schema__(:dump)

    data =
      for {field, {_source, type}} <- types, into: %{} do
        value = Map.get(data, field)

        case dumper.(type, value) do
          {:ok, dumped} -> {"#{field}", dumped}
        end
      end

    {:ok, data}
  end

  def dump(data, dumper, %{cardinality: :many, using: struct}) do
    dumped_list =
      Enum.map(data, fn datum ->
        {:ok, dumped} = dump(datum, dumper, %{cardinality: :one, using: struct})

        dumped
      end)

    {:ok, dumped_list}
  end

  def dump(_data, _dumper, _opts), do: :error

  # From Ecto.Type.
  @doc false
  @impl Ecto.ParameterizedType
  def embed_as(_format, _params), do: :dump
end
