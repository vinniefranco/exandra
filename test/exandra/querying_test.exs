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
      field(:my_udt, Exandra.Types.UDT, type: :fullname)
    end

    def changeset(attrs) do
      cast(%__MODULE__{}, attrs, [:my_string, :my_bool, :my_udt])
    end
  end

  defmodule CounterSchema do
    use Exandra.Table

    @primary_key false
    schema "my_schema" do
      field(:my_string, :binary_id, primary_key: true)
      field(:my_counter, Exandra.Types.XCounter)
    end

    def changeset(attrs) do
      cast(%__MODULE__{}, attrs, [:my_string, :my_counter])
    end
  end

  import Mox

  setup :verify_on_exit!

  describe "querying" do
    test "counters" do
      assert {"SELECT my_string, my_counter FROM my_schema WHERE my_string = 'uuidish'", []} =
               CounterSchema
               |> where([s], s.my_string == "uuidish")
               |> to_xanrda_sql(:all)
    end

    test "select" do
      assert {"SELECT my_string FROM my_schema WHERE my_bool = FALSE", []} =
               Schema
               |> select([s], s.my_string)
               |> where([s], s.my_bool == false)
               |> to_xanrda_sql(:all)
    end

    test "where" do
      assert {"SELECT id, my_string, my_dt, my_bool, my_udt FROM my_schema WHERE my_bool = TRUE LIMIT 1",
              []} =
               Schema
               |> where([s], s.my_bool == true)
               |> limit(1)
               |> to_xanrda_sql(:all)

      nowish = DateTime.utc_now()

      assert {"SELECT id, my_string, my_dt, my_bool, my_udt FROM my_schema WHERE my_dt < ? LIMIT 1",
              [{"timestamp", %DateTime{}}]} =
               Schema
               |> where([s], s.my_dt < ^nowish)
               |> limit(1)
               |> to_xanrda_sql(:all)
    end

    test "create" do
      expect(Exandra.Adapter.Mock, :execute, fn _conn, stmt, values, _adapter ->
        assert "INSERT INTO my_schema (my_string, id) VALUES (?, ?) " = stmt

        assert [
                 {"text", "string"},
                 {"uuid", _},
               ] = values

        {:ok, %Xandra.Void{}}
      end)

      %Schema{}
      |> Ecto.Changeset.cast(%{my_string: "string"}, [:my_string])
      |> Exandra.TestRepo.insert()
    end

    test "update" do
      uuid = Ecto.UUID.generate()
      record = %Schema{id: uuid, my_bool: false}

      expect(Exandra.Adapter.Mock, :execute, fn _conn, stmt, values, _adapter ->
        assert "UPDATE my_schema SET my_bool = ?, my_udt = ? WHERE id = ?" = stmt

        assert [
                 {"boolean", true},
                 {"fullname", %{"first_name" => "frank", "last_name" => "beans"}},
                 {"uuid", ^uuid}
               ] = values

        {:ok, %Xandra.Void{}}
      end)

      record
      |> Ecto.Changeset.cast(
        %{my_bool: "true", my_udt: %{"first_name" => "frank", "last_name" => "beans"}},
        [:my_bool, :my_udt]
      )
      |> Exandra.TestRepo.update()
    end

    test "update counters" do
      uuid = Ecto.UUID.generate()

      expect(Exandra.Adapter.Mock, :execute, fn _conn, stmt, values, _adapter ->
        assert "UPDATE my_schema SET my_counter = ? WHERE my_string = ?" = stmt

        assert [
                 {"counter", 5},
                 {"uuid", ^uuid}
               ] = values

        {:ok, %Xandra.Void{}}
      end)

      record = %CounterSchema{my_string: uuid, my_counter: 4}

      record
      |> Ecto.Changeset.cast(
        %{my_counter: 5},
        [:my_counter]
      )
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
