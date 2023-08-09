defmodule Exandra.BatchTest do
  use ExUnit.Case, async: true

  alias Exandra.Batch

  defmodule Comment do
    use Ecto.Schema

    schema "comments" do
      field :x, :integer
      field :parent_x, :integer
    end
  end

  defmodule Doll do
    use Ecto.Schema

    schema "comments" do
      field :name, :string
    end
  end

  test "new/0" do
    assert %Batch.Builder{} = Batch.new()
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
  end
end
