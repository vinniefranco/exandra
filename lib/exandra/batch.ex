defmodule Exandra.Batch do
  @moduledoc """
  A struct that represents a batch of queries to be executed in a single
  request to Cassandra/Scylla.

  This is essentially a wrapper around `Xandra.Batch`, but you should not
  use `Xandra.Batch` if working with Exandra.

  `Exandra.Batch` contains a single field, `:queries`, which is a list of
  tuples in the form `{query, params}`.

  ## Usage

  To execute a batch of queries through Exandra, you can use the
  `Ecto.Adapters.SQL.query/4` (or `Ecto.Adapters.SQL.query!/4`) function
  and pass in a `Exandra.Batch` struct as the query. Alternatively, you
  can use the `query/4` and `query!/4` functions that Ecto defines in your
  `Ecto.Repo`. For example:

      batch = %Exandra.Batch{
        queries: [
          {"INSERT INTO users (email) VALUES (?)", ["jeff@example.com"]},
          {"INSERT INTO users (email) VALUES (?)", ["britta@example.com"]}
        ]
      }

      MyRepo.query!(batch)

  """

  @enforce_keys [:queries]
  defstruct [:queries]
end
