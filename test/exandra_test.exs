defmodule ExandraTest do
  use Exandra.AdapterCase

  defmodule Schema do
    use Exandra.Table

    alias Exandra.XMap
    alias Exandra.XSet
    alias Exandra.UDT

    @primary_key {:id, :binary_id, autogenerate: true}
    schema "my_schema" do
      field(:my_map, :map)
      field(:my_enum, Ecto.Enum, values: [:foo, :bar], default: :bar)
      field(:my_xmap, XMap, key: :string, value: :integer)
      field(:my_xset, XSet, type: :integer)
      field(:my_udt, UDT, type: :fullname)
      field(:my_list, {:array, :string})
      field(:my_utc, :utc_datetime)
      field(:my_integer, :integer)
      field(:my_bool, :boolean)
      field(:my_decimal, :decimal)

      timestamps(type: :utc_datetime)
    end

    def changeset(attrs) do
      cast(%__MODULE__{}, attrs, [
        :my_enum,
        :my_map,
        :my_xmap,
        :my_udt,
        :my_xset,
        :my_list,
        :my_utc,
        :my_integer,
        :my_bool,
        :my_decimal
      ])
    end
  end

  import Mox

  setup :verify_on_exit!

  describe "insert/1" do
    test "it coerces as expected for the Xandra driver" do
      decimal = Decimal.new("1.0")
      set = MapSet.new([1, 2, 3])
      nowish = DateTime.utc_now()

      expect(Exandra.Adapter.Mock, :execute, fn _conn, stmt, values, _ ->
        assert "INSERT INTO my_schema (my_bool, my_decimal, my_enum, my_integer, my_list, my_map, my_utc, my_xmap, my_xset, inserted_at, updated_at, id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " ==
                 stmt

        assert [
                 {"boolean", false},
                 {"decimal", ^decimal},
                 {"text", "foo"},
                 {"int", 4},
                 {"list<text>", ~w(a b c)},
                 {"text", ~s({"a":"b"})},
                 {"timestamp", %DateTime{}},
                 {"map<text, int>", %{"string" => 1}},
                 {"set<int>", ^set},
                 {"timestamp", %DateTime{}},
                 {"timestamp", %DateTime{}},
                 {"uuid", uuid_binary}
               ] = values

        {:ok,
         %Xandra.Page{
           columns: ~w(id my_map my_xmap my_xset my_list),
           content: [
             [
               uuid_binary,
               ~s({"a":"b"}),
               "foo",
               %{"this" => 1},
               [1, 2, 3],
               ["a", "b", "c"],
               nowish,
               nowish,
               nowish
             ]
           ]
         }}
      end)

      assert {:ok,
              %Schema{
                id: id,
                my_map: %{a: :b},
                my_utc: %DateTime{},
                my_xset: ^set,
                my_list: ["a", "b", "c"],
                inserted_at: %DateTime{},
                updated_at: %DateTime{}
              }} =
               %{
                 my_map: %{a: :b},
                 my_enum: "foo",
                 my_xmap: %{"string" => 1},
                 my_xset: [1, 2, 3],
                 my_list: ["a", "b", "c"],
                 my_utc: nowish,
                 my_bool: false,
                 my_decimal: Decimal.new("1.0"),
                 my_integer: 4
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
      expected_stmt =
        "SELECT id, my_map, my_enum, my_xmap, my_xset, my_udt, my_list, my_utc, my_integer, my_bool, my_decimal, inserted_at, updated_at FROM my_schema"

      row1_id = Ecto.UUID.generate()
      row2_id = Ecto.UUID.generate()

      nowish = DateTime.utc_now()

      Exandra.Adapter.Mock
      |> expect(:prepare, fn _conn, stmt, _opts ->
        assert expected_stmt == stmt
        {:ok, %Xandra.Prepared{statement: stmt}}
      end)
      |> expect(:stream_pages!, fn _conn, _, _opts, _fart ->
        [
          %Xandra.Page{
            columns:
              ~w(id my_map my_enum my_xmap my_xset my_udt my_list my_utc my_integer my_bool my_decimal),
            content: [
              [
                row1_id,
                ~s({"a":"b"}),
                "foo",
                %{"this" => 1},
                [1],
                %{"first_name" => "frank", "last_name" => "beans"},
                ~w(a b c),
                nowish,
                4,
                true,
                Decimal.new("1.23"),
                nowish,
                nowish
              ]
            ]
          },
          %Xandra.Page{
            columns:
              ~w(id my_map my_enum my_xmap my_xset my_udt my_list my_utc my_integer my_bool my_decimal),
            content: [
              [
                row2_id,
                ~s({"a":"b"}),
                "bar",
                %{"that" => 2},
                [1, 2, 3],
                %{"first_name" => "frank", "last_name" => "beans"},
                ~w(1 2 3),
                nowish,
                5,
                false,
                Decimal.new("1.23"),
                nowish,
                nowish
              ]
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
                 my_list: ["a", "b", "c"],
                 my_udt: %{"first_name" => "frank", "last_name" => "beans"},
                 my_bool: true,
                 my_integer: 4
               },
               %Schema{
                 id: ^row2_id,
                 my_map: %{"a" => "b"},
                 my_xmap: %{"that" => 2},
                 my_xset: ^second_set,
                 my_list: ["1", "2", "3"],
                 my_udt: %{"first_name" => "frank", "last_name" => "beans"},
                 my_bool: false,
                 my_integer: 5
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
