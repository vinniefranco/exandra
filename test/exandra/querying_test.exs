defmodule Exandra.QueryingTest do
  alias Ecto.Adapter.Schema
  use ExUnit.Case

  use Exandra.AdapterCase

  import Ecto.Query, warn: false

  defmodule Schema do
    use Exandra.Table

    @primary_key {:id, :binary_id, autogenerate: true}
    schema "my_schema" do
      field(:my_string, :string)
      field(:my_dt, :utc_datetime)
      field(:my_bool, :boolean)
    end

    def changeset(attrs) do
      cast(%__MODULE__{}, attrs, [:my_string, :my_bool])
    end
  end

  import Mox

  setup :verify_on_exit!

  describe "querying" do
    test "select" do
      assert {"SELECT my_string FROM my_schema WHERE my_bool = FALSE", []} =
               Schema
               |> select([s], s.my_string)
               |> where([s], s.my_bool == false)
               |> to_xanrda_sql(:all)
    end

    test "where" do
      assert {"SELECT id, my_string, my_dt, my_bool FROM my_schema WHERE my_bool = TRUE LIMIT 1",
              []} =
               Schema
               |> where([s], s.my_bool == true)
               |> limit(1)
               |> to_xanrda_sql(:all)

      nowish = DateTime.utc_now()

      assert {"SELECT id, my_string, my_dt, my_bool FROM my_schema WHERE my_dt < ? LIMIT 1",
              [{"timestamp", %DateTime{}}]} =
               Schema
               |> where([s], s.my_dt < ^nowish)
               |> limit(1)
               |> to_xanrda_sql(:all)
    end

    test "update" do
      uuid = Ecto.UUID.generate()
      record = %Schema{id: uuid, my_bool: false}

      expect(Exandra.Adapter.Mock, :execute, fn _conn, stmt, values, _adapter ->
        assert "UPDATE my_schema SET my_bool = ? WHERE id = ?" = stmt

        assert [{"boolean", true}, {"uuid", ^uuid}] = values

        {:ok, %Xandra.Void{}}
      end)

      record
      |> Ecto.Changeset.cast(%{my_bool: "true"}, [:my_bool])
      |> Exandra.TestRepo.update()
    end

    test "delete" do
      uuid = Ecto.UUID.generate()

      Exandra.Adapter.Mock
      |> expect(:prepare, fn _conn, stmt, _adapter ->
        assert "DELETE FROM my_schema WHERE id = ?" = stmt

        {:ok, %Xandra.Prepared{}}
      end)
      |> expect(:stream_pages!, fn _conn, %Xandra.Prepared{}, values, _adapter ->
        assert [{"uuid", ^uuid}] = values
        []
      end)

      Schema
      |> where([s], s.id == ^uuid)
      |> Exandra.TestRepo.delete_all()
    end
  end

  defp to_xanrda_sql(queryable, kind) do
    Ecto.Adapters.SQL.to_sql(kind, Exandra.TestRepo, queryable)
  end
end
