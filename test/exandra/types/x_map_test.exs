defmodule Exandra.XMapTest do
  use ExUnit.Case, async: true

  alias Exandra.XMap

  defmodule Schema do
    use Ecto.Schema

    schema "my_schema" do
      field(:my_int_key_xmap, XMap, key: :integer, value: :string)
      field(:my_int_value_xmap, XMap, key: :string, value: :integer)
      field(:my_int_xmap, XMap, key: :integer, value: :integer)
      field(:my_atom_xmap, XMap, key: :atom, value: :integer)
    end
  end

  test "init" do
    assert {
             :parameterized,
             XMap,
             %{
               field: :my_atom_xmap,
               key: :atom,
               schema: Schema,
               value: :integer
             }
           } = Schema.__schema__(:type, :my_atom_xmap)
  end

  @p_dump_type {:parameterized, XMap, XMap.params(:dump)}
  @p_self_type {:parameterized, XMap, XMap.params(:self)}

  test "operations" do
    assert :x_map = Ecto.Type.type(@p_self_type)
    assert :x_map = Ecto.Type.type(@p_dump_type)

    assert :self = Ecto.Type.embed_as(@p_self_type, :foo)
    assert :self = Ecto.Type.embed_as(@p_dump_type, :foo)

    type = Schema.__schema__(:type, :my_atom_xmap)
    assert {:ok, %{}} = Ecto.Type.load(type, :my_atom_xmap)

    type = Schema.__schema__(:type, :my_int_xmap)
    assert Ecto.Type.dump(type, %{1 => 3}) == {:ok, %{1 => 3}}
  end

  test "type/1 returns :x_map" do
    assert :x_map = XMap.type(nil)
  end

  describe "cast/2" do
    test "returns {:ok, map()} when first param is nil" do
      assert {:ok, %{}} == XMap.cast(nil, :anything)
    end

    test "returns {:ok, {op, casted}} when first param is a qualifying tuple" do
      assert {:ok, {:add, %{"a" => "a"}}} ==
               XMap.cast({:add, %{"a" => "a"}}, %{key: :string, value: :string})

      assert {:ok, {:remove, %{"a" => "a"}}} ==
               XMap.cast({:remove, %{"a" => "a"}}, %{key: :string, value: :string})
    end

    test "errors" do
      assert :error = XMap.cast({:anything, %{"a" => "a"}}, %{key: :string, value: :string})
    end
  end

  test "load/3" do
    assert {:ok, %{"string" => "value"}} =
             XMap.load(%{"string" => "value"}, nil, %{key: :string, value: :string})

    assert {:ok, %{}} = XMap.load(nil, nil, %{key: :string, value: :string})
  end

  describe "equal?/3" do
    refute XMap.equal?({nil, nil}, nil, nil)
    refute XMap.equal?(nil, {nil, nil}, nil)
    assert XMap.equal?(nil, nil, :anything)
    assert XMap.equal?(nil, [], :anything)
    assert XMap.equal?([], nil, :anything)
    assert XMap.equal?(%{"a" => "a"}, %{"a" => "a"}, :anything)
    refute XMap.equal?(%{"a" => "a"}, %{"b" => "a"}, :anything)
    refute XMap.equal?(true, true, nil)
  end

  test "embed_as/1 returns :self" do
    assert :self == XMap.embed_as(nil)
  end

  test "xandra_type/1" do
    assert "map<text, text>" == XMap.xandra_type(%{key: :string, value: :string})
  end
end
