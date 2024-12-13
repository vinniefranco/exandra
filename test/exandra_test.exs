defmodule ExandraTest do
  use Exandra.AdapterCase

  import Mox

  alias Exandra.TestRepo

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

  describe "execute_batch/3" do
    setup do
      start_supervised!({TestRepo, nodes: ["localhost:#{@port}"], keyspace: "test"})
      :ok
    end

    test "successfully executes a batch of queries, prepares each unique query only once" do
      queries = [
        {"INSERT INTO users (email) VALUES (?)", ["user1@test.com"]},
        {"INSERT INTO users (email) VALUES (?)", ["user2@test.com"]}
      ]

      XandraMock
      |> expect(:prepare, fn _conn, _stmt, _opts -> {:ok, %Xandra.Prepared{}} end)
      |> expect(:execute, fn _conn, _batch, _opts -> {:ok, %Xandra.Void{}} end)

      XandraClusterMock
      |> expect(:run, fn _cluster, _opts, fun -> fun.(_conn = nil) end)

      assert :ok = Exandra.execute_batch(TestRepo, %Exandra.Batch{queries: queries})
    end

    test "returns error when prepare fails" do
      queries = [{"INSERT INTO users (email) VALUES (?)", ["test@test.com"]}]
      prepare_error = %Xandra.Error{reason: :invalid_syntax}

      XandraMock
      |> expect(:prepare, fn _conn, _stmt, _opts -> {:error, prepare_error} end)

      XandraClusterMock
      |> expect(:run, fn _cluster, _opts, fun -> fun.(_conn = nil) end)

      assert {:error, ^prepare_error} =
               Exandra.execute_batch(TestRepo, %Exandra.Batch{queries: queries})
    end

    test "returns error when execute fails" do
      queries = [{"INSERT INTO users (email) VALUES (?)", ["test@test.com"]}]
      execute_error = %Xandra.Error{reason: :unavailable}

      XandraMock
      |> expect(:prepare, fn _conn, _stmt, _opts -> {:ok, %Xandra.Prepared{}} end)
      |> expect(:execute, fn _conn, _batch, _opts -> {:error, execute_error} end)

      XandraClusterMock
      |> expect(:run, fn _cluster, _opts, fun -> fun.(_conn = nil) end)

      assert {:error, ^execute_error} =
               Exandra.execute_batch(TestRepo, %Exandra.Batch{queries: queries})
    end
  end
end
