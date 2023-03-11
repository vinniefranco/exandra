defmodule Exandra.QueryingTest do
  use ExUnit.Case

  use Exandra.AdapterCase

  defmodule Schema do
    use Exandra.Table

    alias Exandra.Types.XMap
    alias Exandra.Types.XSet

    @primary_key {:id, :binary_id, autogenerate: true}
    schema "my_schema" do
      field(:my_string, :string)
      field(:my_bool, :boolean)
    end

    def changeset(attrs) do
      cast(%__MODULE__{}, attrs, [:my_string, :my_bool])
    end
  end

  import Mox

  setup :verify_on_exit!
end
