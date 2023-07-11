defmodule Exandra.UDT do
  opts_schema = [
    type: [
      type: :atom,
      required: true,
      doc: "The UDT."
    ],
    encoded_fields: [
      type: {:list, :atom},
      doc: "JSON encoded fields."
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
  `Ecto.Type` for **User-Defined Types** (UDTs).

  ## Options

  #{NimbleOptions.docs(opts_schema)}

  ## Examples

  For example, if you have defined an `email` UDT in your database, you can
  use it in your schema like this:

      schema "users" do
        field :email, Exandra.UDT, type: :email
      end

  """

  use Ecto.ParameterizedType

  @opts_schema NimbleOptions.new!(opts_schema)

  @impl Ecto.ParameterizedType
  def type(_params), do: :udt

  @impl Ecto.ParameterizedType
  def init(opts) do
    opts
    |> __validate__()
    |> Map.new()
  end

  @impl Ecto.ParameterizedType
  def cast(data, %{type: _udt}), do: {:ok, data}

  @impl Ecto.ParameterizedType
  def load(data, _loader, params) do
    {:ok, coerce_data(data, params, :load)}
  end

  @impl Ecto.ParameterizedType
  def dump(data, _dumper, params) do
    # Stringify all keys.
    data = for {field, value} <- data || %{}, into: %{}, do: {"#{field}", value}
    data = coerce_data(data, params, :dump)

    {:ok, data}
  end

  @doc false
  defp coerce_data(data, params, type) do
    data = data || %{}

    if fields_to_encode = params[:encoded_fields] do
      for field <- fields_to_encode, into: data do
        stringified_field = Atom.to_string(field)
        {stringified_field, json_parse(data, stringified_field, type)}
      end
    else
      data
    end
  end

  @doc false
  def json_parse(data, field, :dump),
    do: data |> Map.get(field, %{}) |> Jason.encode!()

  @doc false
  def json_parse(data, field, :load),
    do: data |> Map.get(field, "{}") |> Jason.decode!()

  @doc false
  def __validate__(opts), do: NimbleOptions.validate!(opts, @opts_schema)
end
