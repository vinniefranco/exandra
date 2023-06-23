defmodule Exandra.XSetTest do
  use ExUnit.Case, async: true

  alias Exandra.XSet

  defmodule Schema do
    use Ecto.Schema

    schema "my_schema" do
      field(:my_set, XSet, type: :uuid)
    end
  end

  test "init" do
    assert {
             :parameterized,
             XSet,
             %{
               field: :my_set,
               type: :uuid,
               schema: Schema
             }
           } = Schema.__schema__(:type, :my_set)
  end

  @p_dump_type {:parameterized, XSet, XSet.params(:dump)}
  @p_self_type {:parameterized, XSet, XSet.params(:self)}

  test "operations" do
    assert :x_set = Ecto.Type.type(@p_self_type)
    assert :x_set = Ecto.Type.type(@p_dump_type)

    assert :self = Ecto.Type.embed_as(@p_self_type, :foo)
    assert :self = Ecto.Type.embed_as(@p_dump_type, :foo)

    type = Schema.__schema__(:type, :my_set)
    assert {:ok, %{}} = Ecto.Type.load(type, :my_set)

    set = MapSet.new([Ecto.UUID.generate()])
    assert Ecto.Type.dump(type, set) == {:ok, set}
  end

  test "params/1 returns embed with given value as key" do
    assert %{embed: :string} = XSet.params(:string)
  end

  test "type/1 returns :x_set" do
    assert :x_set = XSet.type(:anything)
  end

  test "cast/2" do
    assert {:ok, MapSet.new()} == XSet.cast(nil, :anything)

    assert {:ok, {:add, MapSet.new(["this", "that"])}} ==
             XSet.cast({:add, ["this", "that"]}, %{type: :string})

    assert {:ok, {:remove, MapSet.new(["this", "that"])}} ==
             XSet.cast({:remove, ["this", "that"]}, %{type: :string})

    assert :error = XSet.cast({:remove, :a}, %{type: :string})
    assert {:ok, MapSet.new(["a"])} == XSet.cast(MapSet.new(["a"]), %{type: :string})
    assert {:ok, MapSet.new(["a"])} == XSet.cast(["a"], %{type: :string})
    assert :error = XSet.cast([1], %{type: :string})
    assert {:ok, MapSet.new([1])} == XSet.cast(1, %{type: :integer})
    assert :error = XSet.cast(:asd, nil)
  end

  test "equal?/3" do
    refute XSet.equal?({nil, nil}, nil, nil)
    refute XSet.equal?(nil, {nil, nil}, nil)
    assert XSet.equal?(nil, nil, :anything)
    assert XSet.equal?(nil, [], :anything)
    assert XSet.equal?([], nil, :anything)
    assert XSet.equal?(MapSet.new(["a"]), MapSet.new(["a"]), :anything)
    refute XSet.equal?(MapSet.new(["b"]), MapSet.new(["a"]), :anything)
    refute XSet.equal?(true, true, nil)
  end

  test "embed_as/1 returns :self" do
    assert :self == XSet.embed_as(nil)
  end

  test "xandra_type/1" do
    assert "set<text>" == XSet.xandra_type(%{type: :string})
  end
end
