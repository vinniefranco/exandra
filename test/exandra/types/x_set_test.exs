defmodule Exandra.Types.XSetTest do
  use ExUnit.Case, async: true

  alias Exandra.Types.XSet

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
               schema: Schema,
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

    assert {:ok, :my_int_xmap} == Ecto.Type.dump(type, :my_int_xmap)
  end
end
