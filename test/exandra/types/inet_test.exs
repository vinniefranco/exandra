defmodule Exandra.InetTest do
  use Exandra.AdapterCase, async: true

  alias Exandra.Inet

  test "cast/1" do
    assert {:ok, {127, 0, 0, 1}} == Ecto.Type.cast(Inet, {127, 0, 0, 1})
    assert {:ok, {127, 0, 0, 1}} == Ecto.Type.cast(Inet, "127.0.0.1")
    assert {:ok, {127, 0, 0, 1}} == Ecto.Type.cast(Inet, ~c"127.0.0.1")
    assert :error == Ecto.Type.cast(Inet, {1_234_567, 0, 0, 1})
    assert :error == Ecto.Type.cast(Inet, {127, 0, 0, "1"})

    assert {:ok, {0, 0, 0, 0, 0, 0, 0, 1}} == Ecto.Type.cast(Inet, {0, 0, 0, 0, 0, 0, 0, 1})
    assert {:ok, {0, 0, 0, 0, 0, 0, 0, 1}} == Ecto.Type.cast(Inet, "::1")
    assert {:ok, {0, 0, 0, 0, 0, 0, 0, 1}} == Ecto.Type.cast(Inet, ~c"::1")
    assert :error == Ecto.Type.cast(Inet, {0xFFFF0, 0, 0, 0, 0, 0, 0, 1})
    assert :error == Ecto.Type.cast(Inet, "ffff0::1")
  end

  test "load/1" do
    assert {:ok, {127, 0, 0, 1}} == Ecto.Type.load(Inet, {127, 0, 0, 1})
    assert {:ok, {0, 0, 0, 0, 0, 0, 0, 1}} == Ecto.Type.load(Inet, {0, 0, 0, 0, 0, 0, 0, 1})
    assert :error == Ecto.Type.load(Inet, {1_234_567, 0, 0, 1})
    assert :error == Ecto.Type.load(Inet, {0xFFFF0, 0, 0, 0, 0, 0, 0, 1})
  end

  test "dump/1" do
    assert {:ok, {127, 0, 0, 1}} == Ecto.Type.dump(Inet, {127, 0, 0, 1})
    assert {:ok, {0, 0, 0, 0, 0, 0, 0, 1}} == Ecto.Type.dump(Inet, {0, 0, 0, 0, 0, 0, 0, 1})
    assert :error == Ecto.Type.dump(Inet, {1_234_567, 0, 0, 1})
    assert :error == Ecto.Type.dump(Inet, {0xFFFF0, 0, 0, 0, 0, 0, 0, 1})
  end

  test "type/0" do
    assert Ecto.Type.type(Inet) == :inet
  end
end
