defmodule Exandra.ConnectionTest do
  use ExUnit.Case, async: true

  import Ecto.Query

  alias Ecto.Queryable
  alias Exandra.Connection, as: SQL

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

  defp plan(query, operation \\ :all) do
    {query, _cast_params, _dump_params} = Ecto.Adapter.Queryable.plan_query(operation, Exander, query)
  end

  defp all(query), do: query |> SQL.all() |> IO.iodata_to_binary()
  defp insert(prefix, table, header, rows, on_conflict, returning) do
    IO.iodata_to_binary(SQL.insert(prefix, table, header, rows, on_conflict, returning, []))
  end
end
