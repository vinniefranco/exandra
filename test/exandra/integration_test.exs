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
      field :my_tuple, Exandra.Tuple, types: [:integer, :string]

      field :my_complex_list_udt, {:array, Exandra.UDT},
        type: :my_complex,
        encoded_fields: [:meta]

      field :my_composite, Exandra.Map,
        key: Exandra.Tuple,
        types: [:string, :integer],
        value: :integer

      field :my_complex_udt, Exandra.UDT, type: :my_complex, encoded_fields: [:meta]
      field :my_list, {:array, :string}
      field :my_utc, :utc_datetime_usec
      field :my_integer, :integer
      field :my_bool, :boolean
      field :my_decimal, :decimal
      field :my_inet, Exandra.Tuple, types: [:integer, :integer, :integer, :integer]

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
    opts = [keyspace: @keyspace, nodes: ["localhost:#{@port}"], sync_connect: 1000]

    stub_with_real_modules()
    create_keyspace(@keyspace)

    on_exit(fn ->
      stub_with_real_modules()
      drop_keyspace(@keyspace)
    end)

    {:ok, conn} = Xandra.start_link(Keyword.drop(opts, [:sync_connect]))
    Xandra.execute!(conn, "CREATE TYPE IF NOT EXISTS fullname (first_name text, last_name text)")

    for type <- ["my_complex", "my_embedded_type", "my_embedded_pk"] do
      Xandra.execute!(conn, "DROP TYPE IF EXISTS #{type}")
    end

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
      "CREATE TYPE IF NOT EXISTS my_embedded_pk (id uuid, name text, my_enum text, my_map text)"
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
      my_tuple tuple<int, text>,
      my_composite map<FROZEN<tuple<ascii, int>>, bigint>,
      my_embedded_udt my_embedded_type,
      my_list list<varchar>,
      my_utc timestamp,
      my_integer int,
      my_bool boolean,
      my_decimal decimal,
      my_inet inet,
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

      telemetry_ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:xandra, :execute_query, :stop],
          [:xandra, :prepare_query, :stop]
        ])

      batch = %Exandra.Batch{
        queries: [
          {"INSERT INTO users (email) VALUES (?)", ["bob@example.com"]},
          {"INSERT INTO users (email) VALUES (?)", ["meg@example.com"]}
        ]
      }

      assert Exandra.execute_batch(TestRepo, batch,
               telemetry_metadata: %{some: :value},
               force: true
             ) == :ok

      assert_receive {[:xandra, :prepare_query, :stop], ^telemetry_ref, %{}, %{} = meta}
      assert meta.extra_metadata == %{some: :value}

      assert_receive {[:xandra, :execute_query, :stop], ^telemetry_ref, %{}, %{} = meta}
      assert meta.extra_metadata == %{some: :value}

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
    decimal1 = Decimal.new("3.14")
    decimal2 = Decimal.new("-3.14")
    set1 = MapSet.new([1])
    set2 = MapSet.new([1, 2, 3])
    inet1 = {192, 168, 0, 1}
    inet2 = {10, 0, 0, 1}

    schema1 = %Schema{
      id: row1_id,
      my_map: %{},
      my_exandra_map: %{"this" => 1},
      my_set: set1,
      my_list: ["a", "b", "c"],
      my_udt: %{"first_name" => "frank", "last_name" => "beans"},
      my_list_udt: [%{"first_name" => "frank", "last_name" => "beans"}],
      my_tuple: {1, "foo"},
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
      my_composite: %{{"foo", 1} => 1, {"bar", 8} => 4},
      my_bool: true,
      my_integer: 4,
      my_decimal: decimal1,
      my_inet: inet1
    }

    schema2 = %Schema{
      id: row2_id,
      my_map: %{"a" => "b"},
      my_exandra_map: %{"that" => 2},
      my_set: set2,
      my_list: ["1", "2", "3"],
      my_udt: %{"first_name" => "frank", "last_name" => "beans"},
      my_list_udt: [%{"first_name" => "frank", "last_name" => "beans"}],
      my_tuple: {2, "bar"},
      my_complex_list_udt: [
        %{amount: 4, meta: %{"foo" => "bar"}, happened: ~U[2018-01-01T00:00:00Z]}
      ],
      my_complex_udt: %{
        amount: 8,
        meta: %{"foo" => "bar", "baz" => %{"qux" => "quux"}},
        happened: ~U[2020-01-01T00:00:00Z]
      },
      my_composite: %{{"baz", 0} => 2},
      my_bool: false,
      my_integer: 5,
      my_decimal: decimal2,
      my_inet: inet2
    }

    # Schema with all nils.
    schema3 = %Schema{
      id: nil,
      my_map: nil,
      my_exandra_map: nil,
      my_set: nil,
      my_list: nil,
      my_udt: nil,
      my_list_udt: nil,
      my_tuple: nil,
      my_complex_list_udt: nil,
      my_complex_udt: nil,
      my_composite: nil,
      my_bool: nil,
      # my_integer is used for sorting.
      my_integer: 6,
      my_decimal: nil,
      my_inet: nil
    }

    TestRepo.insert!(schema1)
    TestRepo.insert!(schema2)
    TestRepo.insert!(schema3)

    assert [returned_schema1, returned_schema2, returned_schema3] =
             Schema |> TestRepo.all() |> Enum.sort_by(& &1.my_integer)

    assert %Schema{
             id: ^row1_id,
             my_map: %{},
             my_exandra_map: %{"this" => 1},
             my_set: ^set1,
             my_list: ["a", "b", "c"],
             my_udt: %{"first_name" => "frank", "last_name" => "beans"},
             my_list_udt: [%{"first_name" => "frank", "last_name" => "beans"}],
             my_tuple: {1, "foo"},
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
             my_composite: %{{"bar", 8} => 4, {"foo", 1} => 1},
             my_bool: true,
             my_integer: 4,
             my_decimal: ^decimal1,
             my_inet: ^inet1
           } = returned_schema1

    assert %Schema{
             id: ^row2_id,
             my_map: %{"a" => "b"},
             my_exandra_map: %{"that" => 2},
             my_set: ^set2,
             my_list: ["1", "2", "3"],
             my_udt: %{"first_name" => "frank", "last_name" => "beans"},
             my_list_udt: [%{"first_name" => "frank", "last_name" => "beans"}],
             my_tuple: {2, "bar"},
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
             my_composite: %{{"baz", 0} => 2},
             my_bool: false,
             my_integer: 5,
             my_decimal: ^decimal2,
             my_inet: ^inet2
           } = returned_schema2

    empty_set = MapSet.new()

    assert %Schema{
             id: returned_schema3_id,
             my_map: nil,
             my_exandra_map: %{},
             my_set: ^empty_set,
             my_list: nil,
             my_tuple: nil,
             my_udt: %{},
             my_list_udt: nil,
             my_complex_list_udt: nil,
             my_composite: %{},
             my_complex_udt: %{"meta" => %{}},
             my_bool: nil,
             my_integer: 6,
             my_decimal: nil,
             my_inet: nil
           } = returned_schema3

    assert is_binary(returned_schema3_id)

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

  describe "Telemetry" do
    test "can set extra Telemetry metadata through Ecto's :telemetry_options",
         %{start_opts: start_opts} do
      ref = make_ref()
      event = [:xandra, :execute_query, :stop]
      telemetry_ref = :telemetry_test.attach_event_handlers(self(), [event])

      start_supervised!({TestRepo, start_opts})

      # With a keyword list.
      assert TestRepo.all(Schema, telemetry_options: [ref: ref]) == []

      assert_receive {^event, ^telemetry_ref, %{},
                      %{extra_metadata: %{ref: ^ref, repo: TestRepo}}}

      # With a map.
      assert TestRepo.all(Schema, telemetry_options: %{ref: ref}) == []

      assert_receive {^event, ^telemetry_ref, %{},
                      %{extra_metadata: %{ref: ^ref, repo: TestRepo}}}
    end

    test "raises for invalid :telemetry_options", %{start_opts: start_opts} do
      start_supervised!({TestRepo, start_opts})

      message =
        ~s(Xandra only supports maps or keyword lists for telemetry metadata, got: "invalid extra meta")

      assert_raise ArgumentError, message, fn ->
        TestRepo.all(Schema, telemetry_options: "invalid extra meta") == []
      end
    end
  end

  describe "Embeds" do
    defmodule UDTWithPK do
      use Ecto.Schema
      import Ecto.Changeset

      @primary_key {:id, Ecto.UUID, autogenerate: true}
      embedded_schema do
        field :name, :string
        field :my_enum, Ecto.Enum, values: [:a, :b], default: :a
        field :my_map, :map
      end

      def changeset(entity, params) do
        entity
        |> cast(params, [:name, :my_enum, :my_map])
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
        field :my_pk_udt, Exandra.EmbeddedType, using: UDTWithPK
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
                 my_enum: :a,
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
                   name: "generator",
                   my_map: %{foo: "bar"}
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
                 name: "generator",
                 my_enum: :a,
                 my_map: %{"foo" => "bar"}
               }
             } = TestRepo.one(query)

      assert {:ok, _} = Ecto.UUID.dump(loaded_uuid)
      assert generated_uuid == loaded_uuid
    end
  end
end
