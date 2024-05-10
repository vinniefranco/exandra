defmodule Exandra.TupleTest do
  use Exandra.AdapterCase, async: true

  alias Exandra.Tuple

  test "init" do
    assert {
             :parameterized,
             Tuple,
             %{
               types: [Ecto.UUID, :integer, :string]
             }
           } = Ecto.ParameterizedType.init(Tuple, types: [Ecto.UUID, :integer, :string])
  end

  @p_dump_type {:parameterized, Tuple, Tuple.params(:dump)}
  @p_self_type {:parameterized, Tuple, Tuple.params(:self)}

  test "operations" do
    assert Ecto.Type.type(@p_self_type) == :exandra_tuple
    assert Ecto.Type.type(@p_dump_type) == :exandra_tuple

    assert :self = Ecto.Type.embed_as(@p_self_type, :foo)
    assert :self = Ecto.Type.embed_as(@p_dump_type, :foo)

    type = Ecto.ParameterizedType.init(Tuple, types: [Ecto.UUID, :integer, :string])
    assert {:ok, nil} = Ecto.Type.load(type, :my_tuple)

    binary_uuid = Ecto.UUID.bingenerate()
    string_uuid = Ecto.UUID.cast!(binary_uuid)

    second = :rand.uniform(1000)
    third = :crypto.strong_rand_bytes(20)

    tuple_in = {string_uuid, second, third}
    tuple_out = {binary_uuid, second, third}
    assert Ecto.Type.dump(type, tuple_in) == {:ok, tuple_out}
  end

  test "type/1" do
    assert Tuple.type(nil) == :exandra_tuple
  end

  test "cast/2" do
    assert :error == Tuple.cast(nil, %{types: [:any, :any, :any]})

    assert :error = Tuple.cast({1}, %{types: [:string]})

    assert {:ok, {"a"}} == Tuple.cast({"a"}, %{types: [:string]})
    assert {:ok, {"a"}} == Tuple.cast("a", %{types: [:string]})

    assert :error = Tuple.cast({1}, %{types: [:string]})
    assert {:ok, {1}} == Tuple.cast([1], %{types: [:integer]})

    assert {:ok, {1, "a"}} == Tuple.cast({1, "a"}, %{types: [:integer, :string]})
  end

  test "load/2" do
    assert {:ok, nil} = Tuple.load(nil, %{types: [:string, :string]})
    assert {:ok, {1, "a"}} == Tuple.load({1, "a"}, %{types: [:integer, :string]})
  end

  test "embed_as/1 returns :self" do
    assert :self == Tuple.embed_as(nil)
  end
end
