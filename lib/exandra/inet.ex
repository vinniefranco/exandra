defmodule Exandra.Inet do
  @moduledoc """
  `Ecto.Type` for inets.

  ## Examples

      schema "devices" do
        field :last_ip, Exandra.Inet
      end

  """

  @typedoc """
  The type for an Exandra inet.
  """
  @type t() :: :inet.ip_address()

  @moduledoc since: "0.11.0"

  use Ecto.Type

  @impl Ecto.Type
  def type, do: :inet

  @impl Ecto.Type
  def cast(ip) when is_tuple(ip), do: validate_ip(ip)

  def cast(ip) when is_binary(ip) do
    ip
    |> String.to_charlist()
    |> cast()
  end

  def cast(ip) when is_list(ip) do
    case :inet.parse_address(ip) do
      {:ok, ip} -> {:ok, ip}
      _ -> :error
    end
  end

  def cast(_), do: :error

  @impl Ecto.Type
  def load(ip) when is_tuple(ip), do: validate_ip(ip)

  def load(_), do: :error

  @impl Ecto.Type
  def dump(ip) when is_tuple(ip), do: validate_ip(ip)
  def dump(_), do: :error

  defp validate_ip(ip) do
    if :inet.is_ip_address(ip), do: {:ok, ip}, else: :error
  end
end
