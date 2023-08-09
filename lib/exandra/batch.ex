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

  Alternatively, you can use `Exandra.Batch.new/0` as a fluent interface

      batch =
       Exandra.Batch.new()
       |> Exandra.Batch.insert(:admin, User.changeset(%{email: "jeff@example.com"})
       |> Exandra.Batch.insert(:consumer, User.changeset(%{email: "britta@example.com"})

      Exandra.execute_batch(MyApp.Repo, batch)
  """

  @type t :: %__MODULE__{
          status:
            :default
            | :pending
            | :building
            | :applied,
          queries: [tuple],
          names: MapSet.t(),
          operations: [tuple]
        }

  @enforce_keys [:queries]
  defstruct [:queries, names: MapSet.new(), operations: [], status: :default]

  @doc """
  Returns an empty `Exandra.Batch` struct.

  ## Example

      iex> Exandra.Batch.new() |> Exandra.Batch.to_list()
      []

  """
  @spec new :: t
  def new, do: %__MODULE__{queries: [], status: :building}

  @doc """
  Adds an insert operation to the batch.

  ## Example

      batch =
        Exandra.Batch.new()
        |> Exandra.Batch.insert(:admin, User.changeset(%{email: "jeff@example.com"})

      Exandra.execute_batch(MyApp.Repo, batch)
  """
  @spec insert(t(), atom(), Ecto.Changeset.t()) :: t
  def insert(%__MODULE__{} = batch, name, %Ecto.Changeset{} = changeset) do
    add_changeset(batch, :insert, name, changeset)
  end

  defp add_changeset(%__MODULE__{} = batch, action, name, %Ecto.Changeset{} = changeset) do
    add_operation(batch, name, {:changeset, put_action(changeset, action)})
  end

  defp add_operation(%__MODULE__{operations: operations, names: names} = batch, name, operation) do
    if MapSet.member?(names, name) do
      raise "#{Kernel.inspect(name)} is already a member of the Exandra.Batch: \n#{Kernel.inspect(batch)}"
    else
      %{
        batch
        | operations: [{name, operation} | operations],
          names: MapSet.put(names, name),
          status: :pending
      }
    end
  end

  @doc """
  Returns the list of operations stored in `Batch`.

  Always use this function when you need to access the operations you
  have defined in `Exandra.Batch`. Inspecting the `Exandra.Batch` struct internals
  directly is discouraged.
  """
  @spec to_list(t()) :: [{atom(), term()}]
  def to_list(%__MODULE__{operations: operations}) do
    operations
    |> Enum.reverse()
    |> Enum.map(&format_operation/1)
  end

  defp format_operation({name, {:changeset, changeset}}),
    do: {name, {changeset.action, changeset}}

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

  def __apply__(%__MODULE__{} = batch, repo) do
    operations = Enum.reverse(batch.operations)

    with {:ok, queries} <- operations_to_queries(operations, repo) do
      %{batch | queries: queries, status: :applied}
    end
  end

  defp operations_to_queries([], _repo), do: {:ok, []}

  defp operations_to_queries(operations, repo) do
    me = self()

    Enum.reduce_while(operations, {:ok, []}, fn {name, operation}, acc ->
      apply_operation(operation, name, repo, me, acc)
    end)
  end

  defp apply_operation({:changeset, %{valid?: false} = changeset}, name, _repo, _me, _acc) do
    {:halt, {:error, name, changeset}}
  end

  defp apply_operation({:changeset, changeset}, name, repo, me, {_, result}) do
    case apply(repo, changeset.action, [changeset, [send_raw: me]]) do
      {:ok, _result} ->
        receive do
          {:raw, sql, params} -> {:cont, {:ok, [{sql, params} | result]}}
        end

      {:error, error} ->
        {:halt, {:error, name, error}}
    end
  end
end
