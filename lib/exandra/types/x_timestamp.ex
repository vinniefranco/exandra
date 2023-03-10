defmodule Exandra.Types.XTimestamp do
  @moduledoc """
  Truncates timestamp to milliseconds for Scylla
  """
  @behaviour Ecto.Type

  use Timex

  @impl Ecto.Type
  def type, do: :timestamp

  @impl Ecto.Type
  def cast(nil), do: {:ok, nil}

  def cast(input) when is_binary(input) do
    case Timex.parse(input, "{RFC3339}") do
      {:ok, %DateTime{} = t} -> {:ok, t}
      {:ok, %NaiveDateTime{} = t} -> {:ok, Timex.to_datetime(t)}
      {:error, _} -> :error
    end
  end

  def cast(%DateTime{} = t), do: {:ok, t}

  def cast(dt) do
    case Timex.to_datetime(dt) do
      {:error, _} -> :error
      dt -> cast(dt)
    end
  end

  @impl Ecto.Type
  def load(nil), do: {:ok, nil}
  def load(%DateTime{} = t), do: {:ok, t}
  def load(_), do: :error

  @impl Ecto.Type
  def dump(nil), do: {:ok, nil}
  def dump(%DateTime{} = t), do: {:ok, t}
  def dump(_), do: :error

  @impl Ecto.Type
  def autogenerate do
    DateTime.truncate(Timex.now(), :millisecond)
  end

  @impl Ecto.Type
  def equal?(nil, nil), do: true
  def equal?(_, nil), do: false
  def equal?(nil, _), do: false

  def equal?(%DateTime{} = t1, %DateTime{} = t2) do
    DateTime.compare(
      DateTime.truncate(t1, :millisecond),
      DateTime.truncate(t2, :millisecond)
    ) == :eq
  end

  def equal?(_, _), do: false

  @impl Ecto.Type
  def embed_as(_), do: :self
end
