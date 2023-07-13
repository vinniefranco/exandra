defmodule Exandra.IntegrationTest do
  use Exandra.AdapterCase, async: false, integration: true

  import Ecto.Query

  alias Exandra.TestRepo

  @keyspace "exandra_test"

  defmodule Schema do
    use Ecto.Schema

    @primary_key {:id, :binary_id, autogenerate: true}
    schema "my_schema" do
      field :my_map, :map
      field :my_enum, Ecto.Enum, values: [:foo, :bar], default: :bar
      field :my_exandra_map, Exandra.Map, key: :string, value: :integer
      field :my_set, Exandra.Set, type: :integer
      field :my_udt, Exandra.UDT, type: :fullname
      field :my_list_udt, {:array, Exandra.UDT}, type: :fullname

      field :my_complex_list_udt, {:array, Exandra.UDT},
        type: :my_complex,
        encoded_fields: [:meta]

      field :my_complex_udt, Exandra.UDT, type: :my_complex, encoded_fields: [:meta]
      field :my_list, {:array, :string}
      field :my_utc, :utc_datetime_usec
      field :my_integer, :integer
      field :my_bool, :boolean
      field :my_decimal, :decimal

      timestamps type: :utc_datetime
    end
  end

  defmodule CounterSchema do
    use Ecto.Schema

    @primary_key {:id, :binary_id, autogenerate: true}
    schema "my_counter_schema" do
      field :my_counter, Exandra.Counter
    end
  end

  setup_all do
    opts = [keyspace: @keyspace, hostname: "localhost", port: @port, sync_connect: 1000]

    stub_with_real_modules()
    create_keyspace(@keyspace)

    on_exit(fn ->
      stub_with_real_modules()
      drop_keyspace(@keyspace)
    end)

    {:ok, conn} = Xandra.start_link(Keyword.drop(opts, [:keyspace]))
    Xandra.execute!(conn, "USE #{@keyspace}")
    Xandra.execute!(conn, "CREATE TYPE IF NOT EXISTS fullname (first_name text, last_name text)")

    Xandra.execute!(
      conn,
      "DROP TYPE IF EXISTS my_complex"
    )

    Xandra.execute!(
      conn,
      "CREATE TYPE IF NOT EXISTS my_complex (meta text, amount int, happened timestamp)"
    )

    Xandra.execute!(
      conn,
      "CREATE TYPE IF NOT EXISTS my_embedded_type (dark_mode boolean, online boolean)"
    )

    Xandra.execute!(
      conn,
      "CREATE TYPE IF NOT EXISTS my_embedded_pk (id uuid, name text)"
    )

    for schema <- ["my_schema", "my_embedded_schema", "my_counter_schema"] do
      Xandra.execute!(conn, "DROP TABLE IF EXISTS #{schema}")
    end

    Xandra.execute!(conn, """
    CREATE TABLE my_schema (
      id uuid,
      my_map text,
      my_enum varchar,
      my_exandra_map map<varchar, int>,
      my_set set<int>,
      my_udt fullname,
      my_list_udt FROZEN<list<FROZEN<fullname>>>,
      my_complex_list_udt list<FROZEN<my_complex>>,
      my_complex_udt my_complex,
      my_embedded_udt my_embedded_type,
      my_list list<varchar>,
      my_utc timestamp,
      my_integer int,
      my_bool boolean,
      my_decimal decimal,
      inserted_at timestamp,
      updated_at timestamp,
      PRIMARY KEY (id)
    )
    """)

    Xandra.execute!(conn, """
    CREATE TABLE my_embedded_schema (
      my_name text,
      my_bool boolean,
      my_embedded_udt my_embedded_type,
      my_embedded_udt_list FROZEN<list<FROZEN<my_embedded_type>>>,
      my_pk_udt my_embedded_pk,
      PRIMARY KEY (my_name)
    )
    """)

    Xandra.execute!(conn, """
    CREATE TABLE my_counter_schema (
      id uuid,
      my_counter counter,
      PRIMARY KEY (id)
    )
    """)

    :ok = Xandra.stop(conn)

    %{start_opts: opts}
  end

  setup do
    truncate_all_tables(@keyspace)
    :ok
  end

  describe "all/1" do
    test "returns empty list when no rows exist", %{start_opts: start_opts} do
      start_supervised!({TestRepo, start_opts})
      assert TestRepo.all(Schema) == []
    end
  end

  describe "Exandra.execute_batch/3" do
    test "executes a batch query", %{start_opts: start_opts} do
      start_supervised!({Exandra.TestRepo, start_opts})

      TestRepo.query!("CREATE TABLE IF NOT EXISTS users (email varchar, PRIMARY KEY (email))")

      batch = %Exandra.Batch{
        queries: [
          {"INSERT INTO users (email) VALUES (?)", ["bob@example.com"]},
          {"INSERT INTO users (email) VALUES (?)", ["meg@example.com"]}
        ]
      }

      assert Exandra.execute_batch(TestRepo, batch) == :ok

      assert %{num_rows: 2, rows: rows} = TestRepo.query!("SELECT email FROM users")
      assert rows == [["bob@example.com"], ["meg@example.com"]]
    end
  end

  describe "Exandra.stream!/4" do
    test "executes the given query and returns the Xandra.PageStream", %{
      start_opts: start_opts
    } do
      start_supervised!({Exandra.TestRepo, start_opts})

      TestRepo.query!("CREATE TABLE IF NOT EXISTS users (email varchar, PRIMARY KEY (email))")
      TestRepo.query!("INSERT INTO users (email) VALUES (?)", ["bob@example.com"])
      TestRepo.query!("INSERT INTO users (email) VALUES (?)", ["meg@example.com"])

      assert %Xandra.PageStream{} = stream = Exandra.stream!(TestRepo, "SELECT * FROM users", [])

      emails =
        Enum.flat_map(
          stream,
          fn page -> Enum.map(page, & &1["email"]) end
        )

      assert "bob@example.com" in emails
      assert "meg@example.com" in emails
    end

    test "passes the given options to Xandra.prepare!/4 and Xandra.execute!/4", %{
      start_opts: start_opts
    } do
      start_supervised!({Exandra.TestRepo, start_opts})

      TestRepo.query!("CREATE TABLE IF NOT EXISTS users (email varchar, PRIMARY KEY (email))")

      for email <- ~w[bob@example.com meg@example.com peter@example.com] do
        TestRepo.query!("INSERT INTO users (email) VALUES (?)", [email])
      end

      assert %Xandra.PageStream{} =
               stream =
               Exandra.stream!(
                 TestRepo,
                 "SELECT * FROM users",
                 [],
                 page_size: 2,
                 tracing: true
               )

      [_page1, _page2] = pages = Enum.to_list(stream)
      emails = Enum.flat_map(pages, &Enum.map(&1, fn %{"email" => email} -> email end))

      assert "bob@example.com" in emails
      assert "meg@example.com" in emails
      assert "peter@example.com" in emails
    end

    test "prepares the values correctly", %{start_opts: start_opts} do
      start_supervised!({Exandra.TestRepo, start_opts})

      TestRepo.query!("CREATE TABLE IF NOT EXISTS users (email varchar, PRIMARY KEY (email))")

      for email <- ~w[bob@example.com meg@example.com] do
        TestRepo.query!("INSERT INTO users (email) VALUES (?)", [email])
      end

      assert [%{"email" => "bob@example.com"}] =
               TestRepo
               |> Exandra.stream!("SELECT * FROM users WHERE email = ?", ["bob@example.com"])
               |> Enum.flat_map(&Enum.to_list/1)
    end
  end

  test "inserting and querying data", %{start_opts: start_opts} do
    start_supervised!({TestRepo, start_opts})

    row1_id = Ecto.UUID.generate()
    row2_id = Ecto.UUID.generate()
    set1 = MapSet.new([1])
    set2 = MapSet.new([1, 2, 3])

    schema1 = %Schema{
      id: row1_id,
      my_map: %{},
      my_exandra_map: %{"this" => 1},
      my_set: set1,
      my_list: ["a", "b", "c"],
      my_udt: %{"first_name" => "frank", "last_name" => "beans"},
      my_list_udt: [%{"first_name" => "frank", "last_name" => "beans"}],
      my_complex_list_udt: [
        %{
          amount: 8,
          meta: %{"foo" => "bar", "baz" => %{"qux" => "quux"}},
          happened: ~U[2020-01-01T00:00:00Z]
        }
      ],
      my_complex_udt: %{
        amount: 8,
        meta: %{"foo" => "bar", "baz" => %{"qux" => "quux"}},
        happened: ~U[2020-01-01T00:00:00Z]
      },
      my_bool: true,
      my_integer: 4
    }

    schema2 = %Schema{
      id: row2_id,
      my_map: %{"a" => "b"},
      my_exandra_map: %{"that" => 2},
      my_set: set2,
      my_list: ["1", "2", "3"],
      my_udt: %{"first_name" => "frank", "last_name" => "beans"},
      my_list_udt: [%{"first_name" => "frank", "last_name" => "beans"}],
      my_complex_list_udt: [
        %{amount: 4, meta: %{"foo" => "bar"}, happened: ~U[2018-01-01T00:00:00Z]}
      ],
      my_complex_udt: %{
        amount: 8,
        meta: %{"foo" => "bar", "baz" => %{"qux" => "quux"}},
        happened: ~U[2020-01-01T00:00:00Z]
      },
      my_bool: false,
      my_integer: 5
    }

    TestRepo.insert!(schema1)
    TestRepo.insert!(schema2)

    assert [returned_schema1, returned_schema2] =
             Schema |> TestRepo.all() |> Enum.sort_by(& &1.my_integer)

    assert %Schema{
             id: ^row1_id,
             my_map: %{},
             my_exandra_map: %{"this" => 1},
             my_set: ^set1,
             my_list: ["a", "b", "c"],
             my_udt: %{"first_name" => "frank", "last_name" => "beans"},
             my_list_udt: [%{"first_name" => "frank", "last_name" => "beans"}],
             my_complex_list_udt: [
               %{
                 "amount" => 8,
                 "meta" => %{"foo" => "bar", "baz" => %{"qux" => "quux"}},
                 "happened" => ~U[2020-01-01T00:00:00.000Z]
               }
             ],
             my_complex_udt: %{
               "amount" => 8,
               "meta" => %{"foo" => "bar", "baz" => %{"qux" => "quux"}},
               "happened" => ~U[2020-01-01T00:00:00.000Z]
             },
             my_bool: true,
             my_integer: 4
           } = returned_schema1

    assert %Schema{
             id: ^row2_id,
             my_map: %{"a" => "b"},
             my_exandra_map: %{"that" => 2},
             my_set: ^set2,
             my_list: ["1", "2", "3"],
             my_udt: %{"first_name" => "frank", "last_name" => "beans"},
             my_list_udt: [%{"first_name" => "frank", "last_name" => "beans"}],
             my_complex_list_udt: [
               %{
                 "amount" => 4,
                 "meta" => %{"foo" => "bar"},
                 "happened" => ~U[2018-01-01T00:00:00.000Z]
               }
             ],
             my_complex_udt: %{
               "amount" => 8,
               "meta" => %{"foo" => "bar", "baz" => %{"qux" => "quux"}},
               "happened" => ~U[2020-01-01T00:00:00.000Z]
             },
             my_bool: false,
             my_integer: 5
           } = returned_schema2

    counter_id = Ecto.UUID.generate()

    query =
      from c in CounterSchema,
        update: [set: [my_counter: c.my_counter + 3]],
        where: c.id == ^counter_id

    assert {1, _} = TestRepo.update_all(query, [])

    assert %CounterSchema{} = fetched_counter = TestRepo.get!(CounterSchema, counter_id)
    assert fetched_counter.id == counter_id
    assert fetched_counter.my_counter == 3

    # Let's run the query again and see the counter updated, since that's what counters do.
    assert {1, _} = TestRepo.update_all(query, [])
    assert TestRepo.reload!(fetched_counter).my_counter == 6
  end

  describe "Embeds" do
    defmodule UDTWithPK do
      use Ecto.Schema
      import Ecto.Changeset

      @primary_key {:id, Ecto.UUID, autogenerate: true}
      embedded_schema do
        field :name, :string
      end

      def changeset(entity, params) do
        entity
        |> cast(params, [:name])
      end
    end

    defmodule EmbeddedSchema do
      use Ecto.Schema
      import Ecto.Changeset

      @primary_key false
      embedded_schema do
        field :online, :boolean
        field :dark_mode, :boolean
      end

      def changeset(entity, params) do
        entity
        |> cast(params, [:online, :dark_mode])
        |> validate_required([:online, :dark_mode])
      end
    end

    defmodule MyEmbeddedSchema do
      use Ecto.Schema
      import Ecto.Changeset
      import Exandra, only: [embedded_type: 2, embedded_type: 3]

      @primary_key false
      schema "my_embedded_schema" do
        field :my_name, :string, primary_key: true
        field :my_bool, :boolean
        embedded_type(:my_embedded_udt, EmbeddedSchema)
        embedded_type(:my_embedded_udt_list, EmbeddedSchema, cardinality: :many)
        embedded_type(:my_pk_udt, UDTWithPK)
      end

      def changeset(entity, params) do
        entity
        |> cast(params, [:my_name, :my_bool, :my_embedded_udt, :my_embedded_udt_list, :my_pk_udt])
      end
    end

    test "changeset errors" do
      assert {:error, %Ecto.Changeset{errors: errors}} =
               %MyEmbeddedSchema{}
               |> MyEmbeddedSchema.changeset(%{
                 "my_name" => "EmBetty",
                 "my_bool" => true,
                 "my_embedded_udt" => %{
                   # waffle is not in fact, a boolean
                   "dark_mode" => "waffle",
                   "online" => true
                 }
               })
               |> Ecto.Changeset.apply_action(:insert)

      assert [my_embedded_udt: {"is invalid", _}] = errors
    end

    test "inserting and querying data", %{start_opts: start_opts} do
      start_supervised!({TestRepo, start_opts})

      %MyEmbeddedSchema{}
      |> MyEmbeddedSchema.changeset(%{
        "my_name" => "EmBetty",
        "my_bool" => true,
        "my_embedded_udt" => %{
          "dark_mode" => false,
          "online" => true
        }
      })
      |> TestRepo.insert!()

      assert %MyEmbeddedSchema{
               my_name: "EmBetty",
               my_bool: true,
               my_embedded_udt: %EmbeddedSchema{
                 dark_mode: false,
                 online: true
               },
               my_embedded_udt_list: []
             } = TestRepo.get!(MyEmbeddedSchema, "EmBetty")

      query =
        from e in MyEmbeddedSchema,
          where: e.my_name == "EmBetty"

      assert %MyEmbeddedSchema{
               my_name: "EmBetty",
               my_bool: true,
               my_embedded_udt: %EmbeddedSchema{
                 dark_mode: false,
                 online: true
               },
               my_embedded_udt_list: []
             } = schema = TestRepo.one(query)

      assert %MyEmbeddedSchema{
               my_name: "EmBetty",
               my_bool: false,
               my_embedded_udt: %EmbeddedSchema{
                 dark_mode: false,
                 online: true
               },
               my_embedded_udt_list: [
                 %EmbeddedSchema{dark_mode: true, online: true},
                 %EmbeddedSchema{dark_mode: false, online: false}
               ],
               my_pk_udt: %UDTWithPK{
                 id: generated_uuid,
                 name: "generator"
               }
             } =
               schema
               |> MyEmbeddedSchema.changeset(%{
                 my_bool: false,
                 my_embedded_udt_list: [
                   %{
                     dark_mode: true,
                     online: true
                   },
                   %{
                     dark_mode: false,
                     online: false
                   }
                 ],
                 my_pk_udt: %{
                   name: "generator"
                 }
               })
               |> TestRepo.update!()

      assert %MyEmbeddedSchema{
               my_name: "EmBetty",
               my_bool: false,
               my_embedded_udt: %EmbeddedSchema{
                 dark_mode: false,
                 online: true
               },
               my_embedded_udt_list: [
                 %EmbeddedSchema{dark_mode: true, online: true},
                 %EmbeddedSchema{dark_mode: false, online: false}
               ],
               my_pk_udt: %UDTWithPK{
                 id: loaded_uuid,
                 name: "generator"
               }
             } = TestRepo.one(query)

      assert {:ok, _} = Ecto.UUID.dump(loaded_uuid)
      assert generated_uuid == loaded_uuid
    end
  end
end
