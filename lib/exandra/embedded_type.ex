defmodule Exandra.EmbeddedType do
  use Ecto.ParameterizedType

  @impl Ecto.ParameterizedType
  def init(opts), do: opts

  @impl Ecto.ParameterizedType
  def type(_params), do: :exandra_embedded_type

  @impl Ecto.ParameterizedType
  def cast(nil, _), do: {:ok, %{}}

  @impl Ecto.ParameterizedType
  def load(nil, _loader, _opts), do: {:ok, nil}

  def load(data, _loader, opts) do
    mod = Keyword.get(opts, :into)
    atomified_data = for {field, value} <- data || %{}, into: %{}, do: {String.to_existing_atom(field), value}
    struct = struct(mod, atomified_data)
    {:ok, struct}
  end

  @impl Ecto.ParameterizedType
  def dump(nil, _dumper, _opts), do: {:ok, nil}
  def dump(data, _dumper, _opts) do
    %mod{} = data
    fields = mod.__schema__(:fields) ++ mod.__schema__(:primary_key)

    data = for field <- fields, into: %{}, do: {"#{field}", Map.get(data, field)}

    {:ok, data}
  end
end
