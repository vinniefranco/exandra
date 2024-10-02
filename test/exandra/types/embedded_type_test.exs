defmodule Exandra.EmbeddedTypeTest do
  use Exandra.AdapterCase, async: true

  alias Exandra.EmbeddedType

  defmodule EmbeddedSchema do
    use Ecto.Schema
    import Ecto.Changeset

    @primary_key false
    embedded_schema do
      field :online, :boolean
      field :dark_mode, :boolean
    end

    def changeset(entity, params) do
      entity
      |> cast(params, [:online, :dark_mode])
    end
  end

  defmodule Schema do
    use Ecto.Schema
    import Ecto.Changeset
    import Exandra, only: [embedded_type: 2]

    @primary_key false
    schema "my_embedded_schema" do
      embedded_type(:my_embedded_udt, EmbeddedSchema)
    end

    def changeset(entity, params) do
      entity
      |> cast(params, [:my_name, :my_bool])
      |> cast_embed(:my_embedded_udt)
    end
  end

  test "init" do
    assert {
             :parameterized,
             {EmbeddedType,
             %Exandra.EmbeddedType{
               cardinality: :one,
               field: :my_embedded_udt,
               using: EmbeddedSchema
             }
           }} = Schema.__schema__(:type, :my_embedded_udt)
  end

  @p_dump_type {:parameterized, {EmbeddedType, EmbeddedType.params(:dump)}}
  @p_self_type {:parameterized, {EmbeddedType, EmbeddedType.params(:self)}}

  test "operations" do
    assert Ecto.Type.type(@p_self_type) == :exandra_embedded_type
    assert Ecto.Type.type(@p_dump_type) == :exandra_embedded_type

    assert :dump = Ecto.Type.embed_as(@p_self_type, :foo)
    assert :dump = Ecto.Type.embed_as(@p_dump_type, :foo)

    type = Schema.__schema__(:type, :my_embedded_udt)

    assert {:ok, %EmbeddedSchema{}} =
             Ecto.Type.load(type, %{"dark_mode" => false, "online" => true})
  end

  test "type/1" do
    assert EmbeddedType.type(nil) == :exandra_embedded_type
  end
end
