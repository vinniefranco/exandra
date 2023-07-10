defmodule Exandra.TypesTest do
  use ExUnit.Case, async: true

  alias Exandra.Types

  test "for/1" do
    assert Types.for(:binary_id) == {:ok, "uuid"}
    assert Types.for(:id) == {:ok, "uuid"}
    assert Types.for(:uuid) == {:ok, "uuid"}
    assert Types.for(:integer) == {:ok, "int"}
    assert Types.for({:parameterized, Ecto.Embedded, nil}) == {:ok, "text"}
    assert Types.for({:parameterized, Ecto.Enum, nil}) == {:ok, "text"}
    assert Types.for(:string) == {:ok, "text"}
    assert Types.for(:text) == {:ok, "text"}
    assert Types.for(:naive_datetime) == {:ok, "timestamp"}
    assert Types.for(:naive_datetime_usec) == {:ok, "timestamp"}
    assert Types.for(:utc_datetime) == {:ok, "timestamp"}
    assert Types.for(:utc_datetime_usec) == {:ok, "timestamp"}
    assert Types.for({:array, :string}) == {:ok, "list<text>"}
    assert Types.for({:array, {:array, :int}}) == {:ok, "list<list<int>>"}
    assert Types.for({:array, Exandra.UDT}, type: :full_name) == {:ok, "FROZEN<list<FROZEN<full_name>>>"}
    assert Types.for(:"whatever type!") == {:ok, "whatever type!"}
    assert Types.for(Exandra.UDT, type: :full_name) == {:ok, "FROZEN<full_name>"}

    assert Types.for({:map, :string}) == :error

    # With types that are modules that export the type/0 callback.
    assert Types.for(Exandra.Counter) == {:ok, "counter"}
  end
end
