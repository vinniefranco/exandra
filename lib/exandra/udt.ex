defmodule Exandra.UDT do
  opts_schema = [
    type: [
      type: :atom,
      required: true,
      doc: "The UDT."
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
    |> NimbleOptions.validate!(@opts_schema)
    |> Map.new()
  end

  @impl Ecto.ParameterizedType
  def cast(data, %{type: _udt}), do: {:ok, data}

  @impl Ecto.ParameterizedType
  def load(data, _loader, _params) do
    {:ok, data}
  end

  @impl Ecto.ParameterizedType
  def dump(data, _dumper, _params) do
    {:ok, data}
  end
end
