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

    assert Types.for({:array, Exandra.UDT}, type: :full_name) ==
             {:ok, "FROZEN<list<FROZEN<full_name>>>"}

    assert Types.for(:"whatever type!") == {:ok, "whatever type!"}
    assert Types.for(Exandra.UDT, type: :full_name) == {:ok, "FROZEN<full_name>"}

    assert Types.for({:map, :string}) == :error

    # With types that are modules that export the type/0 callback.
    assert Types.for(Exandra.Counter) == {:ok, "counter"}
  end

  test "check_type!/3" do
    assert Types.check_type!(nil, :string, []) == :string

    assert Types.check_type!(nil, {:array, Exandra.Set}, type: :integer) ==
             {:array, {:parameterized, Exandra.Set, %{type: :integer}}}

    assert Types.check_type!(nil, Exandra.Set, type: :integer) ==
             {:parameterized, Exandra.Set, %{type: :integer}}

    assert Types.check_type!(nil, Exandra.Map, key: Exandra.Set, type: :integer, value: :integer) ==
             {:parameterized, Exandra.Map,
              %{key: {:parameterized, Exandra.Set, %{type: :integer}}, value: :integer}}

    assert Types.check_type!(nil, Exandra.Set, type: Exandra.Set, type: :integer) ==
             {:parameterized, Exandra.Set,
              %{type: {:parameterized, Exandra.Set, %{type: :integer}}}}

    assert Types.check_type!(nil, Exandra.Tuple,
             types: [:integer, Exandra.Set, Exandra.Map],
             type: :string,
             key: :integer,
             value: :binary_id
           ) ==
             {:parameterized, Exandra.Tuple,
              %{
                types: [
                  :integer,
                  {:parameterized, Exandra.Set, %{type: :string}},
                  {:parameterized, Exandra.Map, %{key: :integer, value: :binary_id}}
                ]
              }}

    assert_raise ArgumentError, fn -> Types.check_type!(nil, :nonexsisting, []) end
    assert_raise ArgumentError, fn -> Types.check_type!(nil, {:array, :nonexisting}, []) end
    assert_raise ArgumentError, fn -> Types.check_type!(nil, {:nonexisting, :string}, []) end
    assert_raise ArgumentError, fn -> Types.check_type!(nil, Exandra.Types, []) end
  end
end
