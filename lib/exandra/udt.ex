defmodule Exandra.UDT do
  @moduledoc """
  `Ecto.Type` for **User-Defined Types** (UDTs).
  """

  use Ecto.ParameterizedType

  @opts_schema [
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

  @impl Ecto.ParameterizedType
  def type(_params), do: :udt

  @impl Ecto.ParameterizedType
  def init(opts) do
    opts
    |> NimbleOptions.validate!(@opts_schema)
    |> Map.new()
  end

  @impl Ecto.ParameterizedType
  def cast(data, %{type: udt}) do
    {:ok, {Atom.to_string(udt), data}}
  end

  @impl Ecto.ParameterizedType
  def load(data, _loader, _params) do
    {:ok, data}
  end

  @impl Ecto.ParameterizedType
  def dump(data, _dumper, _params) do
    {:ok, data}
  end
end
