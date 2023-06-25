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

  describe "CREATE TABLE" do
    @describetag :capture_log

    test "with most types",
         %{start_opts: start_opts, unique_keyspace: keyspace, xandra_conn: xandra_conn} do
      defmodule TestMigration do
        use Ecto.Migration

        def change do
          execute("CREATE TYPE fullname (first_name text, last_name text)", "DROP TYPE fullname")

          create_if_not_exists table("most_types", primary_key: false) do
            add :id, :uuid, primary_key: true

            add :my_array, {:array, :int}
            add :my_bigint, :bigint
            add :my_boolean, :boolean
            add :my_decimal, :decimal
            add :my_int, :int
            add :my_map, :map
            add :my_string, :string
            add :my_text, :text
            add :my_tinyint, :tinyint
            add :my_udt, :fullname
            add :my_xmap, :"map<int, boolean>"
            add :my_xset, :"set<uuid>"

            timestamps()
          end

          create_if_not_exists table("just_counters", primary_key: false) do
            add :id, :uuid, primary_key: true
            add :my_xcounter, :counter
          end
        end
      end

      vsn = System.unique_integer([:positive, :monotonic])
      assert Ecto.Migrator.up(TestRepo, vsn, TestMigration) == :ok
      assert Ecto.Migrator.up(TestRepo, vsn, TestMigration) == :already_up

      query = """
      SELECT table_name FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?
      """

      assert %{num_rows: 1} = TestRepo.query!(query, [keyspace, "most_types"])

      query = """
      SELECT column_name, kind, type
      FROM system_schema.columns
      WHERE keyspace_name = ? AND table_name = ?
      """

      assert %{rows: rows} = TestRepo.query!(query, [keyspace, "most_types"])

      assert Enum.sort_by(rows, fn [name, _kind, _type] -> name end) == [
               ["id", "partition_key", "uuid"],
               ["inserted_at", "regular", "timestamp"],
               ["my_array", "regular", "list<int>"],
               ["my_bigint", "regular", "bigint"],
               ["my_boolean", "regular", "boolean"],
               ["my_decimal", "regular", "decimal"],
               ["my_int", "regular", "int"],
               ["my_map", "regular", "text"],
               ["my_string", "regular", "text"],
               ["my_text", "regular", "text"],
               ["my_tinyint", "regular", "tinyint"],
               ["my_udt", "regular", "fullname"],
               ["my_xmap", "regular", "map<int, boolean>"],
               ["my_xset", "regular", "set<uuid>"],
               ["updated_at", "regular", "timestamp"]
             ]

      assert %{rows: rows} = TestRepo.query!(query, [keyspace, "just_counters"])

      assert Enum.sort_by(rows, fn [name, _kind, _type] -> name end) == [
               ["id", "partition_key", "uuid"],
               ["my_xcounter", "regular", "counter"]
             ]
    end

    test "roll back",
         %{start_opts: start_opts, unique_keyspace: keyspace, xandra_conn: xandra_conn} do
      defmodule RollbackMigration do
        use Ecto.Migration

        def change do
          create table("rollbackable", primary_key: false) do
            add :id, :uuid, primary_key: true
          end
        end
      end

      vsn = System.unique_integer([:positive, :monotonic])
      assert Ecto.Migrator.up(TestRepo, vsn, RollbackMigration) == :ok

      assert Ecto.Migrator.down(TestRepo, vsn, RollbackMigration) == :ok
      assert Ecto.Migrator.down(TestRepo, vsn, RollbackMigration) == :already_down

      query = """
      SELECT table_name FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?
      """

      assert %{num_rows: 0} = TestRepo.query!(query, [keyspace, "rollbackable"])
    end
  end
end
