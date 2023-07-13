defmodule Exandra.MapTest do
  use Exandra.AdapterCase, async: true

  alias Exandra.Map

  defmodule Schema do
    use Ecto.Schema

    schema "my_schema" do
      field :my_int_key_map, Exandra.Map, key: :integer, value: :string
      field :my_int_value_map, Exandra.Map, key: :string, value: :integer
      field :my_int_map, Exandra.Map, key: :integer, value: :integer
      field :my_atom_map, Exandra.Map, key: :atom, value: :integer
    end
  end

  test "init" do
    assert {
             :parameterized,
             Map,
             %{
               field: :my_atom_map,
               key: :atom,
               schema: Schema,
               value: :integer
             }
           } = Schema.__schema__(:type, :my_atom_map)
  end

  @p_dump_type {:parameterized, Map, Map.params(:dump)}
  @p_self_type {:parameterized, Map, Map.params(:self)}

  test "operations" do
    assert Ecto.Type.type(@p_self_type) == :exandra_map
    assert Ecto.Type.type(@p_dump_type) == :exandra_map

    assert :self = Ecto.Type.embed_as(@p_self_type, :foo)
    assert :self = Ecto.Type.embed_as(@p_dump_type, :foo)

    type = Schema.__schema__(:type, :my_atom_map)
    assert {:ok, %{}} = Ecto.Type.load(type, :my_atom_map)

    type = Schema.__schema__(:type, :my_int_map)
    assert Ecto.Type.dump(type, %{1 => 3}) == {:ok, %{1 => 3}}
  end

  test "type/1" do
    assert Map.type(nil) == :exandra_map
  end

  describe "cast/2" do
    test "returns {:ok, map()} when first param is nil" do
      assert {:ok, %{}} == Map.cast(nil, :anything)
    end

    test "returns {:ok, {op, casted}} when first param is a qualifying tuple" do
      assert {:ok, {:add, %{"a" => "a"}}} ==
               Map.cast({:add, %{"a" => "a"}}, %{key: :string, value: :string})

      assert {:ok, {:remove, %{"a" => "a"}}} ==
               Map.cast({:remove, %{"a" => "a"}}, %{key: :string, value: :string})
    end

    test "errors" do
      assert :error = Map.cast({:anything, %{"a" => "a"}}, %{key: :string, value: :string})
    end
  end

  test "load/3" do
    assert {:ok, %{"string" => "value"}} =
             Map.load(%{"string" => "value"}, nil, %{key: :string, value: :string})

    assert {:ok, %{}} = Map.load(nil, nil, %{key: :string, value: :string})
  end

  describe "equal?/3" do
    refute Map.equal?({nil, nil}, nil, nil)
    refute Map.equal?(nil, {nil, nil}, nil)
    assert Map.equal?(nil, nil, :anything)
    assert Map.equal?(nil, [], :anything)
    assert Map.equal?([], nil, :anything)
    assert Map.equal?(%{"a" => "a"}, %{"a" => "a"}, :anything)
    refute Map.equal?(%{"a" => "a"}, %{"b" => "a"}, :anything)
    refute Map.equal?(true, true, nil)
  end

  test "embed_as/1 returns :self" do
    assert :self == Map.embed_as(nil)
  end
end
