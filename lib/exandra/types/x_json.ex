defmodule Exandra.Types.XJson do
  use Ecto.ParameterizedType

  @impl Ecto.ParameterizedType
  def type(_), do: :text

  @impl Ecto.ParameterizedType
  def init(opts) do
    Enum.into(opts, %{})
  end

  @impl Ecto.ParameterizedType
  def cast(nil, %{default: default} = opts) when not is_nil(default), do: cast(default, opts)
  def cast(nil, _opts), do: {:ok, nil}
  def cast(%{} = map, _), do: {:ok, map}
  def cast(list, _) when is_list(list), do: {:ok, list}
  def cast(_other, _), do: :error

  @impl Ecto.ParameterizedType
  def load(nil, loader, %{default: default} = opts) when not is_nil(default),
    do: load(default, loader, opts)

  def load(nil, _, _), do: {:ok, nil}
  def load(%{} = map, _, _), do: {:ok, map}
  def load(list, _, _) when is_list(list), do: {:ok, list}

  def load(string, _, opts) when is_binary(string) do
    case Jason.decode(string) do
      {:ok, nil} -> {:ok, Map.get(opts, :default)}
      {:ok, data} -> {:ok, data}
      {:error, _} -> :error
    end
  end

  @impl Ecto.ParameterizedType
  def dump(val), do: {:ok, {"text", Jason.encode!(val)}}

  def dump(nil, dumper, %{default: default} = opts) when not is_nil(default),
    do: dump(default, dumper, opts)

  def dump(nil, _, _), do: {:ok, nil}

  def dump(data, _, _) do
    case Jason.encode(data) do
      {:ok, string} -> {:ok, string}
      {:error, _} -> :error
    end
  end

  @impl Ecto.ParameterizedType
  def equal?(nil, nil, _), do: true
  def equal?(%{} = a, %{} = b, _), do: Map.equal?(a, b)
  def equal?(a, b, _) when is_list(a) and is_list(b), do: a == b
  def equal?(_, _, _), do: false

  def embed_as(_), do: :self
end
