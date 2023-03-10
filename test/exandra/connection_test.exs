defmodule Exandra.ConnectionTest do
  use ExUnit.Case, async: true

  defmodule Schema do
    use Exandra.Table

    alias Exandra.Types.XList
    alias Exandra.Types.XMap
    alias Exandra.Types.XSet

    @primary_key{:id, :binary_id, autogenerate: true}
    schema "schema" do
      field(:my_map, :map)
      field(:my_xmap, XMap, key: :string, value: :integer)
      field(:my_list, XList, type: :uuid)
      field(:my_set, XSet, type: :string)
    end
  end
end
