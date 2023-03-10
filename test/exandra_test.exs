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
      field(:my_list, XList, type: :integer)
      field(:my_set, XSet, type: :string)
    end
  end

  import Mox

  setup :verify_on_exit!

  setup %{conn: conn} do
    {:ok, repo} = Exandra.TestRepo.start_link()

    {:ok, %{conn: conn, repo: repo}}
  end

  describe "all/1" do
    test "returns hydrated Schema structs" do
      expected_stmt = "SELECT id, my_map, my_xmap, my_list, my_set FROM my_schema"
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
            columns: ~w(id my_map my_xmap my_list my_set),
            content: [
              [row1_id, %{}, %{"this" => 1}, [1], ["1"]],
            ]
          },
          %Xandra.Page{
            columns: ~w(id my_map my_xmap my_list my_set),
            content: [
              [row2_id, %{}, %{"that" => 2}, [1, 2, 3], ["another"]],
            ]
          }
        ]
      end)

      assert [
        %Schema{id: ^row1_id},
        %Schema{id: ^row2_id},
      ] = Exandra.TestRepo.all(Schema)
    end
  end
end
