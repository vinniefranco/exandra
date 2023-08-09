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

  defmodule Builder do
    @moduledoc false

    defstruct operations: [], names: MapSet.new()
  end

  @enforce_keys [:queries]
  defstruct [:queries]

  def new, do: %Builder{}

  def insert(%Builder{} = chain, name, %Ecto.Changeset{} = changeset) do
    add_changeset(chain, :insert, name, changeset)
  end

  defp add_changeset(%Builder{} = chain, action, name, %Ecto.Changeset{} = changeset) do
    add_operation(chain, name, {:changeset, put_action(changeset, action)})
  end

  defp add_operation(%Builder{operations: operations, names: names} = chain, name, operation) do
    if MapSet.member?(names, name) do
      raise "#{Kernel.inspect(name)} is already a member of the Exandra.Batch.Builder: \n#{Kernel.inspect(chain)}"
    else
      %{chain | operations: [{name, operation} | operations], names: MapSet.put(names, name)}
    end
  end

  defp put_action(%{action: nil} = changeset, action) do
    %{changeset | action: action}
  end

  defp put_action(%{action: action} = changeset, action) do
    changeset
  end

  defp put_action(%{action: original}, action) do
    raise ArgumentError,
          "you provided a changeset with an action already set " <>
            "to #{Kernel.inspect(original)} when trying to #{action} it"
  end

  def __apply__(%Builder{} = chain, repo) do
    operations = Enum.reverse(chain.operations)

    with {:ok, queries} <- operations_to_queries(operations, repo) do
      %__MODULE__{queries: queries}
    end
  end

  defp operations_to_queries([], _repo), do: {:ok, []}

  defp operations_to_queries(operations, repo) do
    Enum.reduce_while(operations, {:ok, []}, fn {name, operation}, acc ->
      apply_operation(operation, name, repo, acc)
    end)
  end

  defp apply_operation({:changeset, changeset}, name, repo, {_, result}) do
    case apply(repo, changeset.action, [changeset, [send_raw: self()]]) do
      {:ok, _result} ->
        receive do
          {:raw, sql, params} -> {:cont, {:ok, [{sql, params} | result]}}
        end

      {:error, error} ->
        {:halt, {:error, name, error}}
    end
  end
end
