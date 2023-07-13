defmodule Exandra.EmbeddedType do
  use Ecto.ParameterizedType

  # Made public for testing.
  @doc false
  def params(embed), do: %{embed: embed}

  @impl Ecto.ParameterizedType
  def init(opts), do: opts

  @impl Ecto.ParameterizedType
  def type(_params), do: :exandra_embedded_type

  @impl Ecto.ParameterizedType
  def cast(nil, _params) do
    {:ok, nil}
  end

  @impl Ecto.ParameterizedType
  def load(nil, _loader, _opts), do: {:ok, nil}

  def load(%{} = data, _loader, opts) do
    mod = Keyword.get(opts, :into)

    atomified_data =
      for {field, value} <- data || %{}, into: %{}, do: {String.to_existing_atom(field), value}

    struct = struct(mod, atomified_data)
    {:ok, struct}
  end

  def load(_data, _loader, _opts), do: :error

  @impl Ecto.ParameterizedType
  def dump(nil, _dumper, _opts), do: {:ok, nil}

  def dump(%mod{} = data, _dumper, _opts) do
    fields = mod.__schema__(:fields) ++ mod.__schema__(:primary_key)

    data = for field <- fields, into: %{}, do: {"#{field}", Map.get(data, field)}

    {:ok, data}
  end

  def dump(_data, _dumper, _opts), do: :error

  # From Ecto.Type.
  @doc false
  def embed_as(_format), do: :self
end
