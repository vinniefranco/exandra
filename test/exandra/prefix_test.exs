defmodule Exandra.PrefixTest do
  use Exandra.AdapterCase

  import Ecto.Query, warn: false
  import Mox

  alias Exandra.TestRepo

  defmodule MySchema do
    use Ecto.Schema

    @schema_prefix "foo"
    @primary_key {:id, :binary_id, autogenerate: true}
    schema "my_schema" do
      field :my_string, :string
    end
  end

  setup do
    start_link_supervised!({TestRepo, nodes: ["localhost:#{@port}"], keyspace: "unused"})
    :ok
  end

  describe "schema prefix" do
    setup do
      # We're only interested in the prepared statement here, so we stub execution
      stub(XandraMock, :execute, fn _conn, _stmt, _values, _opts -> {:ok, %Xandra.Void{}} end)

      stub(XandraClusterMock, :run, fn _cluster, _opts, fun -> fun.(_conn = nil) end)

      :ok
    end

    test "adds a keyspace to the table" do
      expect(XandraClusterMock, :prepare, fn _conn, stmt, _opts ->
        assert "INSERT INTO foo.my_schema (my_string, id) VALUES (?, ?) " = stmt
        {:ok, %Xandra.Prepared{}}
      end)

      assert {:ok, _} = TestRepo.insert(%MySchema{my_string: "string"})
    end

    test "can be overridden by query options" do
      expect(XandraClusterMock, :prepare, fn _conn, stmt, _options ->
        assert "INSERT INTO bar.my_schema (my_string, id) VALUES (?, ?) " = stmt
        {:ok, %Xandra.Prepared{}}
      end)

      assert {:ok, _} = TestRepo.insert(%MySchema{my_string: "string"}, prefix: "bar")
    end
  end
end
