defmodule Exandra.SetTest do
  use Exandra.AdapterCase, async: true

  alias Exandra.Set

  defmodule Schema do
    use Ecto.Schema

    schema "my_schema" do
      field :my_set, Set, type: Ecto.UUID
    end
  end

  test "init" do
    assert {
             :parameterized,
             Set,
             %{
               field: :my_set,
               type: Ecto.UUID,
               schema: Schema
             }
           } = Schema.__schema__(:type, :my_set)
  end

  @p_dump_type {:parameterized, Set, Set.params(:dump)}
  @p_self_type {:parameterized, Set, Set.params(:self)}

  test "operations" do
    assert Ecto.Type.type(@p_self_type) == :exandra_set
    assert Ecto.Type.type(@p_dump_type) == :exandra_set

    assert :self = Ecto.Type.embed_as(@p_self_type, :foo)
    assert :self = Ecto.Type.embed_as(@p_dump_type, :foo)

    string_uuid = Ecto.UUID.generate()
    binary_uuid = string_uuid |> Ecto.UUID.dump!()

    type = Schema.__schema__(:type, :my_set)
    assert {:ok, %{}} = Ecto.Type.load(type, :my_set)
    assert {:ok, MapSet.new([string_uuid])} == Ecto.Type.load(type, MapSet.new([binary_uuid]))

    assert Ecto.Type.dump(type, MapSet.new([string_uuid])) == {:ok, MapSet.new([binary_uuid])}
  end

  test "params/1 returns embed with given value as key" do
    assert %{embed: :string} = Set.params(:string)
  end

  test "type/1" do
    assert Set.type(:anything) == :exandra_set
  end

  test "cast/2" do
    assert {:ok, MapSet.new()} == Set.cast(nil, :anything)

    assert {:ok, {:add, MapSet.new(["this", "that"])}} ==
             Set.cast({:add, ["this", "that"]}, %{type: :string})

    assert {:ok, {:remove, MapSet.new(["this", "that"])}} ==
             Set.cast({:remove, ["this", "that"]}, %{type: :string})

    assert :error = Set.cast({:remove, :a}, %{type: :string})
    assert {:ok, MapSet.new(["a"])} == Set.cast(MapSet.new(["a"]), %{type: :string})
    assert {:ok, MapSet.new(["a"])} == Set.cast(["a"], %{type: :string})
    assert :error = Set.cast([1], %{type: :string})
    assert {:ok, MapSet.new([1])} == Set.cast(1, %{type: :integer})
    assert {:ok, MapSet.new([1])} == Set.cast([1], %{type: :integer})
    assert :error = Set.cast(:asd, nil)

    assert {:ok, MapSet.new([[1, 2, 3], [4, 5, 6]])} ==
             Set.cast([[1, 2, 3], [4, 5, 6]], %{type: {:array, :integer}})

    assert {:ok, MapSet.new([MapSet.new([1, 2, 3]), MapSet.new([4, 5, 6])])} ==
             Set.cast([[1, 2, 3], [4, 5, 6]], %{type: {:parameterized, Set, %{type: :integer}}})
  end

  test "equal?/3" do
    refute Set.equal?({nil, nil}, nil, nil)
    refute Set.equal?(nil, {nil, nil}, nil)
    assert Set.equal?(nil, nil, :anything)
    assert Set.equal?(nil, [], :anything)
    assert Set.equal?([], nil, :anything)
    assert Set.equal?(MapSet.new(["a"]), MapSet.new(["a"]), :anything)
    refute Set.equal?(MapSet.new(["b"]), MapSet.new(["a"]), :anything)
    refute Set.equal?(true, true, nil)
  end

  test "embed_as/1 returns :self" do
    assert :self == Set.embed_as(nil)
  end
end
