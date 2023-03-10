defmodule ExandraTest do
  use Exandra.AdapterCase

  defmodule Schema do
    use Exandra.Table

    alias Exandra.Types.XList
    alias Exandra.Types.XMap
    alias Exandra.Types.XSet

    @primary_key {:id, :binary_id, autogenerate: true}
    schema "my_schema" do
      field(:my_map, :map)
      field(:my_enum, Ecto.Enum, values: [:foo, :bar], default: :bar)
      field(:my_xmap, XMap, key: :string, value: :integer)
      field(:my_xset, XSet, type: :integer)
      field(:my_xlist, XList, type: :string)
    end

    def changeset(attrs) do
      cast(%__MODULE__{}, attrs, [:my_enum, :my_map, :my_xmap, :my_xset, :my_xlist])
    end
  end

  import Mox

  setup :verify_on_exit!

  describe "insert/1" do
    test "it coerces as expected for the Xandra driver" do
      set = MapSet.new([1, 2, 3])

      expect(Exandra.Adapter.Mock, :execute, fn _conn, stmt, values, _ ->
        assert "INSERT INTO my_schema (my_enum, my_map, my_xlist, my_xmap, my_xset, id) VALUES (?, ?, ?, ?, ?, ?) " ==
                 stmt

        assert [
                 {"text", "foo"},
                 {"text", ~s({"a":"b"})},
                 {"list<text>", ~w(a b c)},
                 {"map<text, int>", %{"string" => 1}},
                 {"set<int>", ^set},
                 {"uuid", uuid_binary}
               ] = values

        {:ok,
         %Xandra.Page{
           columns: ~w(id my_map my_xmap my_xset my_xlist),
           content: [
             [uuid_binary, ~s({"a":"b"}), "foo", %{"this" => 1}, [1, 2, 3], ["a", "b", "c"]]
           ]
         }}
      end)

      assert {:ok, %Schema{id: id, my_map: %{a: :b}, my_xset: ^set, my_xlist: ["a", "b", "c"]}} =
               %{
                 my_map: %{a: :b},
                 my_enum: "foo",
                 my_xmap: %{"string" => 1},
                 my_xset: [1, 2, 3],
                 my_xlist: ["a", "b", "c"]
               }
               |> Schema.changeset()
               |> Exandra.TestRepo.insert()

      assert 36 = String.length(id)
    end
  end

  describe "all/1" do
    test "retuns empty list when no pages exist" do
      Exandra.Adapter.Mock
      |> expect(:prepare, fn _conn, _stmt, _opts -> {:ok, %Xandra.Prepared{}} end)
      |> expect(:stream_pages!, fn _conn, _, _opts, _keyword -> [] end)

      assert Enum.empty?(Exandra.TestRepo.all(Schema))
    end

    test "returns error when stream_pages! raises" do
      Exandra.Adapter.Mock
      |> expect(:prepare, fn _conn, _stmt, _opts ->
        {:ok, %Xandra.Prepared{}}
      end)
      |> expect(:stream_pages!, fn _conn, _, _opts, _keyword ->
        exception = Xandra.ConnectionError.new("connect", "nope")

        raise exception
      end)

      assert_raise Xandra.ConnectionError, fn -> Exandra.TestRepo.all(Schema) end
    end

    test "returns hydrated Schema structs when pages exist" do
      expected_stmt = "SELECT id, my_map, my_enum, my_xmap, my_xset, my_xlist FROM my_schema"
      row1_id = Ecto.UUID.generate()
      row2_id = Ecto.UUID.generate()

      Exandra.Adapter.Mock
      |> expect(:prepare, fn _conn, stmt, _opts ->
        assert expected_stmt == stmt
        {:ok, %Xandra.Prepared{statement: stmt}}
      end)
      |> expect(:stream_pages!, fn _conn, _, _opts, _fart ->
        [
          %Xandra.Page{
            columns: ~w(id my_map my_xmap my_xlist),
            content: [
              [row1_id, %{}, "foo", %{"this" => 1}, [1], ~w(a b c)]
            ]
          },
          %Xandra.Page{
            columns: ~w(id my_map my_xmap my_xlist),
            content: [
              [row2_id, %{"a" => "c"}, "bar", %{"that" => 2}, [1, 2, 3], ~w(1 2 3)]
            ]
          }
        ]
      end)

      first_set = MapSet.new([1])
      second_set = MapSet.new([1, 2, 3])

      assert [
               %Schema{
                 id: ^row1_id,
                 my_map: %{},
                 my_xmap: %{"this" => 1},
                 my_xset: ^first_set,
                 my_xlist: ["a", "b", "c"]
               },
               %Schema{
                 id: ^row2_id,
                 my_map: %{"a" => "c"},
                 my_xmap: %{"that" => 2},
                 my_xset: ^second_set,
                 my_xlist: ["1", "2", "3"]
               }
             ] = Exandra.TestRepo.all(Schema)
    end
  end

  describe "storage_up/1" do
    test "returns :ok when effect is CREATED" do
      Exandra.Adapter.Mock
      |> expect(:start_link, fn _ -> {:ok, self()} end)
      |> expect(:execute, fn _conn, stmt ->
        assert "CREATE KEYSPACE IF NOT EXISTS test\nWITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'}\nAND durable_writes = true;\n" =
                 stmt

        {:ok, %Xandra.SchemaChange{effect: "CREATED"}}
      end)

      assert :ok = Exandra.storage_up(keyspace: "test")
    end

    test "returns {:error, :already_up} when driver returns %Xandra.Void{}" do
      Exandra.Adapter.Mock
      |> expect(:start_link, fn _ -> {:ok, self()} end)
      |> expect(:execute, fn _conn, _stmt -> {:ok, %Xandra.Void{}} end)

      assert {:error, :already_up} = Exandra.storage_up(keyspace: "test")
    end

    test "returns error when result is anything else" do
      Exandra.Adapter.Mock
      |> expect(:start_link, fn _ -> {:ok, self()} end)
      |> expect(:execute, fn _conn, _stmt -> {:error, :anything} end)

      assert {:error, :anything} = Exandra.storage_up(keyspace: "test")
    end
  end

  describe "storage_down/1" do
    test "returns :ok when effect is DROPPED" do
      Exandra.Adapter.Mock
      |> expect(:start_link, fn _ -> {:ok, self()} end)
      |> expect(:execute, fn _conn, stmt ->
        assert "DROP KEYSPACE IF EXISTS test;" = stmt

        {:ok, %Xandra.SchemaChange{effect: "DROPPED"}}
      end)

      assert :ok = Exandra.storage_down(keyspace: "test")
    end

    test "returns {:error, :already_up} when driver returns %Xandra.Void{}" do
      Exandra.Adapter.Mock
      |> expect(:start_link, fn _ -> {:ok, self()} end)
      |> expect(:execute, fn _conn, _stmt -> {:ok, %Xandra.Void{}} end)

      assert {:error, :already_down} = Exandra.storage_down(keyspace: "test")
    end

    test "returns error when result is anything else" do
      Exandra.Adapter.Mock
      |> expect(:start_link, fn _ -> {:ok, self()} end)
      |> expect(:execute, fn _conn, _stmt -> {:error, :anything} end)

      assert {:error, :anything} = Exandra.storage_down(keyspace: "test")
    end
  end

  describe "storage_status/1" do
    test "returns :up when result is not an error" do
      Exandra.Adapter.Mock
      |> expect(:start_link, fn _ -> {:ok, self()} end)
      |> expect(:execute, fn _conn, stmt ->
        assert "USE KEYSPACE test;" = stmt

        {:ok, :greeeaaaat}
      end)

      assert :up = Exandra.storage_status(keyspace: "test")
    end

    test "returns :down when driver returns %Xandra.Error{}" do
      Exandra.Adapter.Mock
      |> expect(:start_link, fn _ -> {:ok, self()} end)
      |> expect(:execute, fn _conn, _stmt -> {:error, %Xandra.Error{reason: :invalid}} end)

      assert :down = Exandra.storage_status(keyspace: "test")
    end

    test "returns {:error, _} as passthru when result not matched" do
      Exandra.Adapter.Mock
      |> expect(:start_link, fn _ -> {:ok, self()} end)
      |> expect(:execute, fn _conn, _stmt -> {:error, :anything} end)

      assert {:error, :anything} = Exandra.storage_status(keyspace: "test")
    end
  end
end
