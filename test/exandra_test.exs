defmodule ExandraTest do
  use Exandra.AdapterCase, integration: false

  import Mox

  describe "storage_up/1" do
    test "returns :ok when effect is CREATED" do
      XandraMock
      |> expect(:start_link, fn _ -> {:ok, self()} end)
      |> expect(:execute, fn _conn, stmt ->
        assert "CREATE KEYSPACE IF NOT EXISTS test\nWITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'}\nAND durable_writes = true;\n" =
                 stmt

        {:ok, %Xandra.SchemaChange{effect: "CREATED"}}
      end)

      assert :ok = Exandra.storage_up(keyspace: "test")
    end

    test "returns {:error, :already_up} when driver returns %Xandra.Void{}" do
      XandraMock
      |> expect(:start_link, fn _ -> {:ok, self()} end)
      |> expect(:execute, fn _conn, _stmt -> {:ok, %Xandra.Void{}} end)

      assert {:error, :already_up} = Exandra.storage_up(keyspace: "test")
    end

    test "returns error when result is anything else" do
      XandraMock
      |> expect(:start_link, fn _ -> {:ok, self()} end)
      |> expect(:execute, fn _conn, _stmt -> {:error, :anything} end)

      assert {:error, :anything} = Exandra.storage_up(keyspace: "test")
    end
  end

  describe "storage_down/1" do
    test "returns :ok when effect is DROPPED" do
      XandraMock
      |> expect(:start_link, fn _ -> {:ok, self()} end)
      |> expect(:execute, fn _conn, stmt ->
        assert stmt == "DROP KEYSPACE IF EXISTS test"

        {:ok, %Xandra.SchemaChange{effect: "DROPPED"}}
      end)

      assert :ok = Exandra.storage_down(keyspace: "test")
    end

    test "returns {:error, :already_up} when driver returns %Xandra.Void{}" do
      XandraMock
      |> expect(:start_link, fn _ -> {:ok, self()} end)
      |> expect(:execute, fn _conn, _stmt -> {:ok, %Xandra.Void{}} end)

      assert {:error, :already_down} = Exandra.storage_down(keyspace: "test")
    end

    test "returns error when result is anything else" do
      XandraMock
      |> expect(:start_link, fn _ -> {:ok, self()} end)
      |> expect(:execute, fn _conn, _stmt -> {:error, :anything} end)

      assert {:error, :anything} = Exandra.storage_down(keyspace: "test")
    end
  end

  describe "storage_status/1" do
    test "returns :up when result is not an error" do
      XandraMock
      |> expect(:start_link, fn _ -> {:ok, self()} end)
      |> expect(:execute, fn _conn, stmt ->
        assert stmt == "USE KEYSPACE test"
        {:ok, :greeeaaaat}
      end)

      assert :up = Exandra.storage_status(keyspace: "test")
    end

    test "returns :down when driver returns %Xandra.Error{}" do
      XandraMock
      |> expect(:start_link, fn _ -> {:ok, self()} end)
      |> expect(:execute, fn _conn, _stmt -> {:error, %Xandra.Error{reason: :invalid}} end)

      assert :down = Exandra.storage_status(keyspace: "test")
    end

    test "returns {:error, _} as passthru when result not matched" do
      XandraMock
      |> expect(:start_link, fn _ -> {:ok, self()} end)
      |> expect(:execute, fn _conn, _stmt -> {:error, :anything} end)

      assert {:error, :anything} = Exandra.storage_status(keyspace: "test")
    end
  end
end
