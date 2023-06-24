defmodule Exandra.IntegrationTest do
  use Exandra.AdapterCase, async: false

  @moduletag :integration

  import Mox

  @repo_app :exandra
  @port String.to_integer(System.get_env("EXANDRA_PORT", "9042"))

  defmodule Repo do
    use Ecto.Repo, otp_app: :exandra, adapter: Exandra
  end

  defmodule Schema do
    use Exandra.Table

    alias Exandra.XMap
    alias Exandra.XSet
    alias Exandra.UDT

    @primary_key {:id, :binary_id, autogenerate: true}
    schema "my_schema" do
      field :my_map, :map
      field :my_enum, Ecto.Enum, values: [:foo, :bar], default: :bar
      field :my_xmap, XMap, key: :string, value: :integer
      field :my_xset, XSet, type: :integer
      field :my_udt, UDT, type: :fullname
      field :my_list, {:array, :string}
      field :my_utc, :utc_datetime_usec
      field :my_integer, :integer
      field :my_bool, :boolean
      field :my_decimal, :decimal

      timestamps type: :utc_datetime
    end

    def changeset(attrs) do
      cast(%__MODULE__{}, attrs, [
        :my_enum,
        :my_map,
        :my_xmap,
        :my_udt,
        :my_xset,
        :my_list,
        :my_utc,
        :my_integer,
        :my_bool,
        :my_decimal
      ])
    end
  end

  setup :set_mox_global
  setup :verify_on_exit!
  setup :stub_with_real_modules
  setup :generate_keyspace

  setup %{keyspace: keyspace} do
    opts = [keyspace: keyspace, hostname: "localhost", port: @port, sync_connect: 1000]
    Application.put_env(@repo_app, Repo, opts)

    xandra_conn =
      opts
      |> Keyword.take([:hostname, :port, :sync_connect])
      |> then(&{Xandra, &1})
      |> Supervisor.child_spec(id: :"xandra_#{keyspace}")
      |> start_link_supervised!()

    Exandra.storage_up(opts)

    on_exit(fn ->
      stub_with_real_modules()
      Exandra.storage_down(opts)
    end)

    %{start_opts: opts, xandra_conn: xandra_conn}
  end

  test "end-to-end flow", %{start_opts: conn_opts} do
    xandra_conn =
      start_link_supervised!({Xandra, Keyword.take(conn_opts, [:hostname, :port, :sync_connect])})

    Xandra.execute!(
      xandra_conn,
      "CREATE KEYSPACE IF NOT EXISTS exandra_integration WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"
    )

    start_supervised!(Repo)

    result = Repo.query!("SELECT * FROM system_schema.keyspaces")
    assert result.num_rows > 0
  end

  test "inserting data", %{start_opts: conn_opts, keyspace: keyspace} do
    nowish = DateTime.utc_now()

    Exandra.storage_up(conn_opts)
    start_supervised!(Repo)

    {:ok, xandra_conn} =
      Xandra.start_link(Keyword.take(conn_opts, [:hostname, :port, :sync_connect]))

    Xandra.execute!(xandra_conn, "USE #{keyspace}")

    Xandra.execute!(xandra_conn, "DROP TABLE IF EXISTS my_schema")

    Xandra.execute!(xandra_conn, """
    CREATE TABLE my_schema (
      id uuid PRIMARY KEY,
      my_map text,
      my_enum text,
      my_xmap map<text, int>,
      my_xset set<int>,
      my_list list<text>,
      my_utc timestamp,
      my_bool boolean,
      my_decimal decimal,
      my_integer int,
      inserted_at timestamp,
      updated_at timestamp
    )
    """)

    assert {:ok, %Schema{} = schema} =
             %{
               my_map: %{a: :b},
               my_enum: "foo",
               my_xmap: %{"string" => 1},
               my_xset: [1, 2, 3],
               my_list: ["a", "b", "c"],
               my_utc: nowish,
               my_bool: false,
               my_decimal: Decimal.new("1.0"),
               my_integer: 4
             }
             |> Schema.changeset()
             |> Repo.insert()

    assert schema.my_map == %{a: :b}
    assert schema.my_enum == :foo
    assert schema.my_xmap == %{"string" => 1}
    assert schema.my_xset == MapSet.new([1, 2, 3])
    assert schema.my_udt == nil
    assert schema.my_list == ["a", "b", "c"]
    assert schema.my_utc == nowish
    assert schema.my_integer == 4
    assert schema.my_bool == false
    assert schema.my_decimal == Decimal.new("1.0")
  end

  @tag :capture_log
  test "can run migrations", %{start_opts: conn_opts, keyspace: keyspace} do
    Exandra.storage_up(conn_opts)

    on_exit(fn ->
      {:ok, xandra_conn} =
        Xandra.start_link(Keyword.take(conn_opts, [:hostname, :port, :sync_connect]))

      Xandra.execute!(xandra_conn, "USE #{keyspace}")

      Xandra.execute!(
        xandra_conn,
        "SELECT table_name FROM system_schema.tables WHERE keyspace_name = '#{keyspace}'"
      )
      |> Enum.each(fn %{"table_name" => table} ->
        Xandra.execute!(xandra_conn, "DROP TABLE IF EXISTS #{table}")
      end)

      Xandra.execute!(xandra_conn, "DROP KEYSPACE IF EXISTS #{keyspace}")
      Xandra.stop(xandra_conn)
    end)

    start_supervised!({Ecto.Migrator, repos: [Repo]})
    start_supervised!(Repo)

    defmodule TestMigration do
      use Ecto.Migration

      def change do
        create_if_not_exists table("exandra_users", primary_key: false) do
          add(:email, :string, primary_key: true)
          add(:age, :integer)
        end
      end
    end

    assert Ecto.Migrator.up(Repo, 1, TestMigration,
             log: false,
             log_migrations_sql: false,
             log_migrator_sql: false
           ) in [
             :ok,
             :already_up
           ]
  end

  defp generate_keyspace(%{test: test_name}) do
    keyspace =
      test_name
      |> Atom.to_string()
      |> String.trim_leading("test ")
      |> String.downcase()
      |> String.replace(~r/([^a-zA-Z0-9_]|\s)/, "_")
      |> then(&"exandra_integration_#{&1}")

    %{keyspace: keyspace}
  end
end
