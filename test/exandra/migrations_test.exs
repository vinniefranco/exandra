defmodule Exandra.MigrationsTest do
  use Exandra.AdapterCase, integration: true

  alias Exandra.TestRepo

  setup %{unique_keyspace: keyspace} do
    opts = [keyspace: keyspace, hostname: "localhost", port: @port, sync_connect: 1000]

    create_keyspace(keyspace)

    on_exit(fn ->
      stub_with_real_modules()
      drop_keyspace(keyspace)
    end)

    start_supervised!({Ecto.Migrator, repos: []})
    start_supervised!({TestRepo, opts})

    xandra_conn = start_link_supervised!({Xandra, Keyword.drop(opts, [:keyspace])})
    Xandra.execute!(xandra_conn, "USE #{keyspace}")

    %{start_opts: opts, xandra_conn: xandra_conn}
  end

  @tag :capture_log
  test "CREATE TABLE",
       %{start_opts: start_opts, unique_keyspace: keyspace, xandra_conn: xandra_conn} do
    defmodule TestMigration do
      use Ecto.Migration

      def change do
        create_if_not_exists table("exandra_users", primary_key: false) do
          add :email, :string, primary_key: true
          add :username, :string, primary_key: true
          add :bio, :string
          add :age, :integer
          timestamps()
        end
      end
    end

    assert Ecto.Migrator.up(TestRepo, 1, TestMigration) == :ok
    assert Ecto.Migrator.up(TestRepo, 1, TestMigration) == :already_up

    query = """
    SELECT table_name FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?
    """

    assert %{num_rows: 1} = TestRepo.query!(query, [keyspace, "exandra_users"])

    query = """
    SELECT column_name, kind, type
    FROM system_schema.columns
    WHERE keyspace_name = ? AND table_name = ?
    """

    assert %{rows: rows} = TestRepo.query!(query, [keyspace, "exandra_users"])

    assert Enum.sort_by(rows, fn [name, _kind, _type] -> name end) == [
             ["age", "regular", "int"],
             ["bio", "regular", "text"],
             ["email", "partition_key", "text"],
             ["inserted_at", "regular", "timestamp"],
             ["updated_at", "regular", "timestamp"],
             ["username", "clustering", "text"]
           ]
  end
end
