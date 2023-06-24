defmodule Exandra.TypesTest do
  use ExUnit.Case, async: true

  alias Exandra.Types

  test "for/1" do
    assert :uuid = Types.for(:binary_id)
    assert :uuid = Types.for(:id)
    assert :int = Types.for(:integer)
    assert :text = Types.for({:parameterized, Ecto.Embedded, nil})
    assert :text = Types.for({:parameterized, Ecto.Enum, nil})
    assert :text = Types.for(:string)
    assert :text = Types.for(:text)

    assert "map<text, int>" =
             Types.for({:parameterized, Exandra.XMap, %{key: :text, value: :integer}})

    assert "set<text>" = Types.for({:parameterized, Exandra.XSet, %{type: :text}})
    assert :x_set = Types.for({:set, {:array, :string}})
  end
end
