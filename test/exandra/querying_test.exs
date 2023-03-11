defmodule Exandra.QueryingTest do
  use ExUnit.Case

  use Exandra.AdapterCase

  import Ecto.Query, warn: false

  defmodule Schema do
    use Exandra.Table

    @primary_key {:id, :binary_id, autogenerate: true}
    schema "my_schema" do
      field(:my_string, :string)
      field(:my_bool, :boolean)
    end

    def changeset(attrs) do
      cast(%__MODULE__{}, attrs, [:my_string, :my_bool])
    end
  end

  import Mox

  setup :verify_on_exit!

  test "select" do
    assert {"SELECT my_string FROM my_schema WHERE my_bool = FALSE", []} =
             Schema
             |> select([s], s.my_string)
             |> where([s], s.my_bool == false)
             |> to_xanrda_sql(:all)
  end

  test "where" do
    assert {"SELECT id, my_string, my_bool FROM my_schema WHERE my_bool = TRUE", []} =
             Schema
             |> where([s], s.my_bool == true)
             |> to_xanrda_sql(:all)
  end

  defp to_xanrda_sql(queryable, kind) do
    Ecto.Adapters.SQL.to_sql(kind, Exandra.TestRepo, queryable)
  end
end
