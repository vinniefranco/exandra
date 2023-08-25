defmodule Exandra.QueryingTest do
  use Exandra.AdapterCase

  import Ecto.Query, warn: false
  import Mox

  alias Exandra.TestRepo

  defmodule MySchema do
    use Ecto.Schema

    @primary_key {:id, :binary_id, autogenerate: true}
    schema "my_schema" do
      field :my_string, :string
      field :my_datetime, :utc_datetime
      field :my_bool, :boolean
      field :my_udt, Exandra.UDT, type: :fullname
      field :my_listed_udt, {:array, Exandra.UDT}, type: :fullname
      field :my_counter, Exandra.Counter
    end
  end

  setup do
    start_link_supervised!({TestRepo, host: "localhost", port: @port, keyspace: "unused"})
    :ok
  end

  describe "querying" do
    test "create" do
      XandraMock
      |> expect(:prepare, fn _conn, stmt, _opts ->
        assert "INSERT INTO my_schema (my_string, id) VALUES (?, ?) " = stmt
        {:ok, %Xandra.Prepared{}}
      end)
      |> expect(:execute, fn _conn, _stmt, values, _opts ->
        assert ["string", _uuid] = values
        {:ok, %Xandra.Void{}}
      end)

      TestRepo.insert(%MySchema{my_string: "string"})
    end

    test "update counters" do
      uuid = Ecto.UUID.generate()

      XandraMock
      |> expect(:prepare, fn _conn, stmt, _options ->
        assert "UPDATE my_schema SET my_counter = ? WHERE id = ?" = stmt
        {:ok, %Xandra.Prepared{}}
      end)
      |> expect(:execute, fn _conn, _stmt, values, _adapter ->
        assert values == [5, Ecto.UUID.dump!(uuid)]
        {:ok, %Xandra.Void{}}
      end)

      record = %MySchema{id: uuid, my_counter: 4}

      record
      |> Ecto.Changeset.cast(
        %{my_counter: 5},
        [:my_counter]
      )
      |> TestRepo.update()
    end
  end

  describe "Ecto.Repo.to_sql/2 implementation" do
    test "with basic SELECT" do
      query = from(s in MySchema, select: s.my_string)
      {sql, params} = TestRepo.to_sql(:all, query)

      assert sql == "SELECT my_string FROM my_schema"
      assert params == []
    end

    test "with WHERE" do
      now = DateTime.truncate(DateTime.utc_now(), :second)
      query = from(s in MySchema, where: s.my_datetime < ^now)

      {sql, params} = TestRepo.to_sql(:all, query)

      assert sql ==
               "SELECT id, my_string, my_datetime, my_bool, my_udt, my_listed_udt, my_counter FROM my_schema WHERE my_datetime < ?"

      assert params == [now]
    end

    test "with WHERE and LIMIT" do
      now = DateTime.truncate(DateTime.utc_now(), :second)
      query = from(s in MySchema, where: s.my_datetime < ^now, limit: 2)

      {sql, params} = TestRepo.to_sql(:all, query)

      assert sql == """
             SELECT id, my_string, my_datetime, my_bool, my_udt, my_listed_udt, my_counter \
             FROM my_schema \
             WHERE my_datetime < ? \
             LIMIT 2\
             """

      assert params == [now]
    end

    test "with LIMIT clauses" do
      limit = 10
      query = from(s in MySchema, limit: ^limit)

      {sql, params} = TestRepo.to_sql(:all, query)

      assert sql =~ "FROM my_schema LIMIT ?"

      assert params == [limit]
    end

    test "with DELETE" do
      uuid = Ecto.UUID.generate()
      query = from(s in MySchema, where: s.id == ^uuid)

      {sql, params} = TestRepo.to_sql(:delete_all, query)
      assert sql == "DELETE FROM my_schema WHERE id = ?"
      assert params == [Ecto.UUID.dump!(uuid)]
    end

    test "with prefix" do
      query = from(s in MySchema, prefix: "prefix")

      {sql, _params} = TestRepo.to_sql(:all, query)
      assert sql =~ "FROM prefix.my_schema"
    end
  end
end
