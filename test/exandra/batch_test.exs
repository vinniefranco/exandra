defmodule Exandra.BatchTest do
  use ExUnit.Case, async: true

  alias Exandra.Batch
  alias Exandra.TestRepo

  defmodule Comment do
    use Ecto.Schema

    schema "comments" do
      field :x, :integer
      field :parent_x, :integer
    end
  end

  defmodule Doll do
    use Ecto.Schema
    import Ecto.Changeset

    @primary_key {:id, :binary_id, autogenerate: true}
    schema "comments" do
      field :name, :string
    end

    def changeset(attrs) do
      %__MODULE__{}
      |> cast(attrs, [:name])
      |> validate_required([:name])
    end
  end

  test "new/0" do
    assert %Batch{status: :building} = Batch.new()
  end

  describe "insert/3" do
    test "changeset" do
      changeset = Ecto.Changeset.change(%Comment{})

      batch =
        Batch.new()
        |> Batch.insert(:comment, changeset)

      assert batch.names == MapSet.new([:comment])
      assert batch.operations == [{:comment, {:changeset, %{changeset | action: :insert}}}]
    end

    test "raises on duplicate name" do
      changeset = Ecto.Changeset.change(%Comment{})

      assert_raise RuntimeError, ~r/:comment is already a member of the Exandra.Batch/, fn ->
        Batch.new()
        |> Batch.insert(:comment, changeset)
        |> Batch.insert(:comment, changeset)
      end
    end

    test "raises when action conflicts with call" do
      changeset = Doll.changeset(%{name: "foo"})
      changeset = %{changeset | action: :update}

      assert_raise ArgumentError,
                   "you provided a changeset with an action already set to :update when trying to insert it",
                   fn ->
                     Batch.insert(Batch.new(), :test, changeset)
                   end
    end

    test "fails with an invalid changeset" do
      batch = Batch.insert(Batch.new(), :nope, Doll.changeset(%{}))

      assert {:error, :nope, error_changeset} = Exandra.execute_batch(TestRepo, batch)
      assert error_changeset.errors == [name: {"can't be blank", [validation: :required]}]
    end
  end

  test "to_list" do
    changeset = Ecto.Changeset.change(%Comment{id: 1}, x: 1)

    batch =
      Batch.new()
      |> Batch.insert(:insert, changeset)
      |> Batch.insert(:insert_1, changeset)
      |> Batch.insert(:insert_2, changeset)

    assert [
             {:insert, {:insert, _}},
             {:insert_1, {:insert, _}},
             {:insert_2, {:insert, _}}
           ] = Batch.to_list(batch)
  end
end
