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
      field(:my_xmap, XMap, key: :string, value: :integer)
    end

    def changeset(attrs) do
      cast(%__MODULE__{}, attrs, [:my_map, :my_xmap])
    end
  end

  import Mox

  setup :verify_on_exit!

  describe "insert/1" do
    test "it coerces as expected for the Xandra driver" do
      expect(Exandra.Adapter.Mock, :execute, fn _conn, stmt, values, _ ->
        assert "INSERT INTO my_schema (my_map, my_xmap, id) VALUES (?, ?, ?) " == stmt

        assert [
                 {"map", %{a: :b}},
                 {"map<text, int>", %{"string" => 1}},
                 {"uuid", uuid_binary}
               ] = values

        {:ok,
         %Xandra.Page{
           columns: ~w(id my_map my_xmap),
           content: [
             [uuid_binary, %{a: :b}, %{"this" => 1}]
           ]
         }}
      end)

      %{my_map: %{a: :b}, my_xmap: %{"string" => 1}, my_xlist: [1, 2], my_xset: ["a", "b"]}
      |> Schema.changeset()
      |> Exandra.TestRepo.insert()
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
      expected_stmt = "SELECT id, my_map, my_xmap FROM my_schema"
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
            columns: ~w(id my_map my_xmap),
            content: [
              [row1_id, %{}, %{"this" => 1}, [1], ["1"]]
            ]
          },
          %Xandra.Page{
            columns: ~w(id my_map my_xmap),
            content: [
              [row2_id, %{}, %{"that" => 2}, [1, 2, 3], ["another"]]
            ]
          }
        ]
      end)

      assert [
               %Schema{id: ^row1_id},
               %Schema{id: ^row2_id}
             ] = Exandra.TestRepo.all(Schema)
    end
  end
end
