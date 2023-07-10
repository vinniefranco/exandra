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
  `Exandra.execute_batch/3`.

      batch = %Exandra.Batch{
        queries: [
          {"INSERT INTO users (email) VALUES (?)", ["jeff@example.com"]},
          {"INSERT INTO users (email) VALUES (?)", ["britta@example.com"]}
        ]
      }

      Exandra.execute_batch(MyApp.Repo, batch)

  """

  @enforce_keys [:queries]
  defstruct [:queries]
end
