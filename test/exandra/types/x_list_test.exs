defmodule Exandra.Types.XListTest do
  use ExUnit.Case, async: true

  alias Exandra.Types.XList

  defmodule Schema do
    use Ecto.Schema

    schema "my_schema" do
      field(:my_list, XList, type: :text)
    end
  end

  test "init" do
    assert {
             :parameterized,
             XList,
             %{
               field: :my_list,
               type: :text,
               schema: Schema
             }
           } = Schema.__schema__(:type, :my_list)
  end

  @p_dump_type {:parameterized, XList, XList.params(:dump)}
  @p_self_type {:parameterized, XList, XList.params(:self)}

  test "operations" do
    assert :x_list = Ecto.Type.type(@p_self_type)
    assert :x_list = Ecto.Type.type(@p_dump_type)

    assert :self = Ecto.Type.embed_as(@p_self_type, :foo)
    assert :self = Ecto.Type.embed_as(@p_dump_type, :foo)

    type = Schema.__schema__(:type, :my_list)
    assert {:ok, []} = Ecto.Type.load(type, :my_list)

    assert {:ok, {"list<text>", :my_list}} == Ecto.Type.dump(type, :my_list)
  end

  describe "cast/2" do
    test "returns expected values" do
      assert {:ok, []} = XList.cast(nil, :anything)
      assert {:ok, {:add, [1]}} = XList.cast({:add, 1}, %{type: :integer})
      assert {:ok, {:remove, [1]}} = XList.cast({:remove, 1}, %{type: :integer})
      assert :error = XList.cast({:nope, 1}, %{type: :integer})

      assert :error = XList.cast([1, :a, "string"], %{type: :string})
      assert {:ok, [1, 2, 3]} = XList.cast([1, 2, 3], %{type: :integer})

      assert :error = XList.cast(:a, nil)
    end
  end

  describe "load/3" do
    test "returns expected values" do
      # This feels wrong
      assert {:ok, []} = XList.load(:atom, nil, %{type: :string})
      assert {:ok, []} = XList.load(nil, nil, %{type: :string})
      assert :error = XList.load([1], nil, %{type: :string})
    end
  end

  test "dump/3" do
    assert {:ok, {"list<text>", ["a"]}} = XList.dump(["a"], nil, %{type: :string})
  end

  describe "equal?/3" do
    refute XList.equal?({nil, nil}, nil, nil)
    refute XList.equal?(nil, {nil, nil}, nil)
    assert XList.equal?(nil, nil, :anything)
    assert XList.equal?(nil, [], :anything)
    assert XList.equal?([], nil, :anything)
    assert XList.equal?(["a"], ["a"], :anything)
    refute XList.equal?(["a"], ["b"], :anything)
    refute XList.equal?(true, true, nil)
  end

  test "embed_as/1 returns :self" do
    assert :self == XList.embed_as(nil)
  end

  test "xandra_type/1" do
    assert "list<text>" == XList.xandra_type(%{type: :string})
  end
end
