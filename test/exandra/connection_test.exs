defmodule Exandra.ConnectionTest do
  use ExUnit.Case, async: true

  import Ecto.Query

  alias Ecto.Migration.Reference
  alias Ecto.Queryable

  alias Exandra.Connection, as: SQL

  defmodule Schema do
    use Ecto.Schema

    schema "schema" do
      field(:x, :integer)
      field(:y, :integer)
      field(:z, :integer)
      field(:meta, :map)
    end
  end

  defp plan(query, operation \\ :all) do
    {query, _cast_params, _dump_params} =
      Ecto.Adapter.Queryable.plan_query(operation, Exandra, query)

    query
  end

  defp all(query), do: query |> SQL.all() |> IO.iodata_to_binary()
  defp update_all(query), do: query |> SQL.update_all() |> IO.iodata_to_binary()
  defp delete_all(query), do: query |> SQL.delete_all() |> IO.iodata_to_binary()
  defp execute_ddl(query), do: query |> SQL.execute_ddl() |> Enum.map(&IO.iodata_to_binary/1)

  defp insert(prefx, table, header, rows, on_conflict, returning) do
    IO.iodata_to_binary(SQL.insert(prefx, table, header, rows, on_conflict, returning, []))
  end

  defp update(prefx, table, fields, filter, returning, opts \\ []) do
    IO.iodata_to_binary(SQL.update(prefx, table, fields, filter, returning, opts))
  end

  defp delete(prefx, table, filter, returning) do
    IO.iodata_to_binary(SQL.delete(prefx, table, filter, returning))
  end

  test "from" do
    query = Schema |> select([r], r.x) |> plan()
    assert all(query) == ~s{SELECT x FROM schema}
  end

  test "from without schema" do
    query = "posts" |> select([r], r.x) |> plan()
    assert all(query) == ~s{SELECT x FROM posts}

    query = "Posts" |> select([:x]) |> plan()
    assert all(query) == ~s{SELECT x FROM Posts}

    query = "0posts" |> select([:x]) |> plan()
    assert all(query) == ~s{SELECT x FROM 0posts}

    assert_raise Ecto.QueryError,
                 ~r"Scylla does not support selecting all fields from posts without a schema",
                 fn ->
                   all(from(p in "posts", select: p) |> plan())
                 end
  end

  test "from with subquery" do
    query = subquery("posts" |> select([r], %{x: r.x, y: r.y})) |> select([r], r.x) |> plan()

    assert_raise Ecto.QueryError,
                 ~r"Exandra does not support subqueries at this time",
                 fn ->
                   all(query)
                 end
  end

  test "from with fragment" do
    query = from(f in fragment("select ? as x", ^"abc"), select: f.x) |> plan()

    assert_raise Ecto.QueryError,
                 ~r"Exandra does not support subqueries at this time",
                 fn ->
                   all(query)
                 end
  end

  test "from with hints" do
    query =
      Schema
      |> from(hints: ["ALLOW FILTERING", "PER PARTITION LIMIT 1"])
      |> select([r], r.x)
      |> where([r], r.y == 7)
      |> plan()

    assert all(query) == "SELECT x FROM schema WHERE y = 7 ALLOW FILTERING PER PARTITION LIMIT 1"
  end

  test "CTE" do
    initial_query =
      "categories"
      |> where([c], is_nil(c.parent_id))
      |> select([c], %{id: c.id, depth: fragment("1")})

    iteration_query =
      "categories"
      |> join(:inner, [c], t in "tree", on: t.id == c.parent_id)
      |> select([c, t], %{id: c.id, depth: fragment("? + 1", t.depth)})

    cte_query = initial_query |> union_all(^iteration_query)

    query =
      Schema
      |> recursive_ctes(true)
      |> with_cte("tree", as: ^cte_query)
      |> join(:inner, [r], t in "tree", on: t.id == r.category_id)
      |> select([r, t], %{x: r.x, category_id: t.id, depth: type(t.depth, :integer)})
      |> plan()

    assert_raise Ecto.QueryError, ~r"Exandra does not support CTEs at this time", fn ->
      all(query)
    end
  end

  @raw_sql_cte """
  SELECT * FROM categories WHERE c.parent_id IS NULL
  UNION ALL
  SELECT * FROM categories AS c, category_tree AS ct WHERE ct.id = c.parent_id
  """

  test "reference CTE in union" do
    comments_scope_query =
      "comments"
      |> where([c], is_nil(c.deleted_at))
      |> select([c], %{entity_id: c.entity_id, text: c.text})

    posts_query =
      "posts"
      |> join(:inner, [p], c in "comments_scope", on: c.entity_id == p.guid)
      |> select([p, c], [p.title, c.text])

    videos_query =
      "videos"
      |> join(:inner, [v], c in "comments_scope", on: c.entity_id == v.guid)
      |> select([v, c], [v.title, c.text])

    query =
      posts_query
      |> union_all(^videos_query)
      |> with_cte("comments_scope", as: ^comments_scope_query)
      |> plan()

    assert_raise Ecto.QueryError, ~r"Exandra does not support CTEs at this time", fn ->
      all(query)
    end
  end

  test "fragment CTE" do
    query =
      Schema
      |> recursive_ctes(true)
      |> with_cte("tree", as: fragment(@raw_sql_cte))
      |> join(:inner, [p], c in "tree", on: c.id == p.category_id)
      |> select([r], r.x)
      |> plan()

    assert_raise Ecto.QueryError, ~r"Exandra does not support CTEs at this time", fn ->
      all(query)
    end
  end

  test "CTE update_all" do
    cte_query =
      from(x in Schema,
        order_by: [asc: :id],
        limit: 10,
        lock: "FOR UPDATE SKIP LOCKED",
        select: %{id: x.id}
      )

    query =
      Schema
      |> with_cte("target_rows", as: ^cte_query)
      |> join(:inner, [row], target in "target_rows", on: target.id == row.id)
      |> update(set: [x: 123])
      |> plan(:update_all)

    assert_raise Ecto.QueryError, ~r"Exandra does not support CTEs at this time", fn ->
      update_all(query)
    end
  end

  test "CTE delete_all" do
    cte_query =
      from(x in Schema,
        order_by: [asc: :id],
        limit: 10,
        lock: "FOR UPDATE SKIP LOCKED",
        select: %{id: x.id}
      )

    query =
      Schema
      |> with_cte("target_rows", as: ^cte_query)
      |> join(:inner, [row], target in "target_rows", on: target.id == row.id)
      |> plan(:delete_all)

    assert_raise Ecto.QueryError, ~r"Exandra does not support CTEs at this time", fn ->
      assert delete_all(query)
    end
  end

  test "all with prefix" do
    query = Schema |> select([r], r.x) |> Ecto.Query.put_query_prefix("prefix") |> plan()

    assert all(query) == ~s{SELECT x FROM prefix.schema}

    query =
      Schema
      |> from(prefix: "first")
      |> select([r], r.x)
      |> Ecto.Query.put_query_prefix("prefix")
      |> plan()

    assert all(query) == ~s{SELECT x FROM first.schema}

    query = "posts" |> from(prefix: "prefix") |> select([r], r.x) |> plan()
    assert all(query) == ~s{SELECT x FROM prefix.posts}
  end

  test "select" do
    query = Schema |> select([r], {r.x, r.y}) |> plan()
    assert all(query) == ~s{SELECT x, y FROM schema}

    query = Schema |> select([r], [r.x, r.y]) |> plan()
    assert all(query) == ~s{SELECT x, y FROM schema}

    query = Schema |> select([r], struct(r, [:x, :y])) |> plan()
    assert all(query) == ~s{SELECT x, y FROM schema}
  end

  test "aggregates" do
    query = Schema |> select(count()) |> plan()
    assert all(query) == ~s{SELECT count(*) FROM schema}
  end

  test "aggregate filters" do
    query = Schema |> select([r], count(r.x) |> filter(r.x > 10)) |> plan()

    assert_raise Ecto.QueryError, ~r/Exandra does not support aggregate filters in query/, fn ->
      all(query)
    end
  end

  test "distinct" do
    query = Schema |> distinct([r], true) |> select([r], {r.x, r.y}) |> plan()
    assert all(query) == ~s{SELECT DISTINCT x, y FROM schema}

    query = Schema |> distinct([r], false) |> select([r], {r.x, r.y}) |> plan()
    assert all(query) == ~s{SELECT x, y FROM schema}

    query = Schema |> distinct(true) |> select([r], {r.x, r.y}) |> plan()
    assert all(query) == ~s{SELECT DISTINCT x, y FROM schema}

    query = Schema |> distinct(false) |> select([r], {r.x, r.y}) |> plan()
    assert all(query) == ~s{SELECT x, y FROM schema}

    assert_raise Ecto.QueryError,
                 ~r"DISTINCT with multiple columns is not supported by Exandra",
                 fn ->
                   query =
                     Schema |> distinct([r], [r.x, r.y]) |> select([r], {r.x, r.y}) |> plan()

                   all(query)
                 end
  end

  test "coalesce" do
    query = Schema |> select([s], coalesce(s.x, 5)) |> plan()

    assert_raise Ecto.QueryError, ~r"COALESCE function is not supported by Exandra", fn ->
      all(query)
    end
  end

  test "where" do
    query = Schema |> where([r], r.x == 42) |> where([r], r.y != 43) |> select([r], r.x) |> plan()
    assert all(query) == ~s{SELECT x FROM schema WHERE x = 42 AND y != 43}

    query = Schema |> where([r], {r.x, r.y} > {1, 2}) |> select([r], r.x) |> plan()
    assert all(query) == ~s{SELECT x FROM schema WHERE (x,y) > (1,2)}
  end

  test "or_where" do
    query =
      Schema |> or_where([r], r.x == 42) |> or_where([r], r.y != 43) |> select([r], r.x) |> plan()

    assert all(query) == ~s{SELECT x FROM schema WHERE x = 42 OR y != 43}
  end

  test "order by" do
    query = Schema |> order_by([r], r.x) |> select([r], r.x) |> plan()
    assert all(query) == ~s{SELECT x FROM schema ORDER BY x ASC}

    query = Schema |> order_by([r], [r.x, r.y]) |> select([r], r.x) |> plan()
    assert all(query) == ~s{SELECT x FROM schema ORDER BY x ASC, y ASC}

    query = Schema |> order_by([r], asc: r.x, desc: r.y) |> select([r], r.x) |> plan()
    assert all(query) == ~s{SELECT x FROM schema ORDER BY x ASC, y DESC}

    query = Schema |> order_by([r], []) |> select([r], r.x) |> plan()
    assert all(query) == ~s{SELECT x FROM schema}

    for dir <- [:asc_nulls_first, :asc_nulls_last, :desc_nulls_first, :desc_nulls_last] do
      assert_raise Ecto.QueryError, ~r"#{dir} is not supported in ORDER BY in Exandra", fn ->
        Schema |> order_by([r], [{^dir, r.x}]) |> select([r], r.x) |> plan() |> all()
      end
    end
  end

  test "union and union all" do
    base_query =
      Schema |> select([r], r.x) |> order_by(fragment("rand")) |> offset(10) |> limit(5)

    union_query1 = Schema |> select([r], r.y) |> order_by([r], r.y) |> offset(20) |> limit(40)
    union_query2 = Schema |> select([r], r.z) |> order_by([r], r.z) |> offset(30) |> limit(60)

    query = base_query |> union(^union_query1) |> union(^union_query2) |> plan()

    assert_raise Ecto.QueryError, ~r"`union` is not supported by Exandra", fn ->
      all(query)
    end

    query = base_query |> union_all(^union_query1) |> union_all(^union_query2) |> plan()

    assert_raise Ecto.QueryError, ~r"`union_all` is not supported by Exandra", fn ->
      all(query)
    end
  end

  test "except and except all" do
    base_query =
      Schema |> select([r], r.x) |> order_by(fragment("rand")) |> offset(10) |> limit(5)

    except_query1 = Schema |> select([r], r.y) |> order_by([r], r.y) |> offset(20) |> limit(40)
    except_query2 = Schema |> select([r], r.z) |> order_by([r], r.z) |> offset(30) |> limit(60)

    query = base_query |> except(^except_query1) |> except(^except_query2) |> plan()

    assert_raise Ecto.QueryError, ~r"`except` is not supported by Exandra", fn ->
      all(query)
    end

    query = base_query |> except_all(^except_query1) |> except_all(^except_query2) |> plan()

    assert_raise Ecto.QueryError, ~r"`except_all` is not supported by Exandra", fn ->
      all(query)
    end
  end

  test "intersect and intersect all" do
    base_query =
      Schema |> select([r], r.x) |> order_by(fragment("rand")) |> offset(10) |> limit(5)

    intersect_query1 = Schema |> select([r], r.y) |> order_by([r], r.y) |> offset(20) |> limit(40)
    intersect_query2 = Schema |> select([r], r.z) |> order_by([r], r.z) |> offset(30) |> limit(60)

    query = base_query |> intersect(^intersect_query1) |> intersect(^intersect_query2) |> plan()

    assert_raise Ecto.QueryError, ~r"`intersect` is not supported by Exandra", fn ->
      all(query)
    end

    query =
      base_query |> intersect_all(^intersect_query1) |> intersect_all(^intersect_query2) |> plan()

    assert_raise Ecto.QueryError, ~r"`intersect_all` is not supported by Exandra", fn ->
      all(query)
    end
  end

  test "limit and offset" do
    query = Schema |> limit([r], 3) |> select([], true) |> plan()
    assert all(query) == ~s{SELECT TRUE FROM schema LIMIT 3}

    query = Schema |> offset([r], 5) |> select([], true) |> plan()

    assert_raise Ecto.QueryError, ~r"`offset` is not supported by Exandra", fn ->
      all(query)
    end

    assert_raise Ecto.QueryError, ~r"`offset` is not supported by Exandra", fn ->
      query = Schema |> offset([r], 5) |> limit([r], 3) |> select([], true) |> plan()
      all(query)
    end
  end

  test "lock" do
    query = Schema |> lock("LOCK IN SHARE MODE") |> select([], true) |> plan()

    assert_raise Ecto.QueryError, ~r"`lock` is not supported by Exandra", fn ->
      all(query)
    end

    query = Schema |> lock([p], fragment("UPDATE on ?", p)) |> select([], true) |> plan()

    assert_raise Ecto.QueryError, ~r"`lock` is not supported by Exandra", fn ->
      all(query)
    end
  end

  test "string escape" do
    query = "schema" |> where(foo: "'\\  ") |> select([], true) |> plan()
    assert all(query) == ~s{SELECT TRUE FROM schema WHERE foo = '''\\\\  '}

    query = "schema" |> where(foo: "'") |> select([], true) |> plan()
    assert all(query) == ~s{SELECT TRUE FROM schema WHERE foo = ''''}
  end

  test "binary ops" do
    query = Schema |> select([r], r.x == 2) |> plan()
    assert all(query) == ~s{SELECT x = 2 FROM schema}

    query = Schema |> select([r], r.x != 2) |> plan()
    assert all(query) == ~s{SELECT x != 2 FROM schema}

    query = Schema |> select([r], r.x <= 2) |> plan()
    assert all(query) == ~s{SELECT x <= 2 FROM schema}

    query = Schema |> select([r], r.x >= 2) |> plan()
    assert all(query) == ~s{SELECT x >= 2 FROM schema}

    query = Schema |> select([r], r.x < 2) |> plan()
    assert all(query) == ~s{SELECT x < 2 FROM schema}

    query = Schema |> select([r], r.x > 2) |> plan()
    assert all(query) == ~s{SELECT x > 2 FROM schema}

    query = Schema |> select([r], r.x + 2) |> plan()
    assert all(query) == ~s{SELECT x + 2 FROM schema}
  end

  test "is_nil" do
    query = Schema |> select([r], is_nil(r.x)) |> plan()
    assert all(query) == ~s{SELECT x IS NULL FROM schema}

    query = Schema |> select([r], not is_nil(r.x)) |> plan()
    assert all(query) == ~s{SELECT NOT (x IS NULL) FROM schema}

    query = "schema" |> select([r], r.x == is_nil(r.y)) |> plan()
    assert all(query) == ~s{SELECT x = y IS NULL FROM schema}
  end

  test "fragments" do
    query = Schema |> select([r], fragment("now")) |> plan()
    assert all(query) == ~s{SELECT now FROM schema}

    query = Schema |> select([r], fragment("intAsBlob(?) AS one", 1)) |> plan()
    assert all(query) == ~s{SELECT intAsBlob(1) AS one FROM schema}

    query = Schema |> select([r], fragment("intAsBlob(?)", r.x)) |> plan()
    assert all(query) == ~s{SELECT intAsBlob(x) FROM schema}

    query = Schema |> select([r], r.x) |> where([], fragment("? = \"query\\?\"", ^10)) |> plan()
    assert all(query) == ~s{SELECT x FROM schema WHERE ? = \"query?\"}

    query = Schema |> select([], fragment(title: 2)) |> plan()

    assert_raise Ecto.QueryError, fn ->
      all(query)
    end
  end

  test "aliasing a selected value with selected_as/2" do
    query = "schema" |> select([s], selected_as(s.x, :integer)) |> plan()
    assert all(query) == ~s{SELECT x AS integer FROM schema}
  end

  test "group_by cannot reference the alias of a selected value with selected_as/1" do
    query =
      "schema"
      |> select([s], selected_as(s.x, :integer))
      |> group_by(selected_as(:integer))
      |> plan()

    assert all(query) == ~s{SELECT x AS integer FROM schema GROUP BY integer}
  end

  test "order_by can reference the alias of a selected value with selected_as/1" do
    query =
      "schema"
      |> select([s], selected_as(s.x, :integer))
      |> order_by(selected_as(:integer))
      |> plan()

    assert all(query) == ~s{SELECT x AS integer FROM schema ORDER BY integer ASC}

    query =
      "schema"
      |> select([s], selected_as(s.x, :integer))
      |> order_by(desc: selected_as(:integer))
      |> plan()

    assert all(query) == ~s{SELECT x AS integer FROM schema ORDER BY integer DESC}
  end

  test "having can reference the alias of a selected value with selected_as/1" do
    query =
      "schema"
      |> select([s], selected_as(s.x, :integer))
      |> group_by(selected_as(:integer))
      |> having(selected_as(:integer) > 0)
      |> plan()

    assert_raise Ecto.QueryError, ~r"HAVING is not supported by Exandra", fn ->
      all(query)
    end
  end

  test "tagged type" do
    query =
      Schema |> select([], type(^"601d74e4-a8d3-4b6e-8365-eddb4c893327", Ecto.UUID)) |> plan()

    assert all(query) == ~s{SELECT CAST(? AS uuid) FROM schema}
  end

  test "string type" do
    query = Schema |> select([], type(^"test", :string)) |> plan()

    assert all(query) == ~s{SELECT CAST(? AS text) FROM schema}
  end

  test "in expression" do
    query = Schema |> select([s], s.x) |> where([s], s.id in ^[1, 2, 3]) |> plan()
    assert all(query) == ~s{SELECT x FROM schema WHERE id IN (?,?,?)}
  end

  test "group by" do
    query = Schema |> group_by([r], r.x) |> select([r], r.x) |> plan()
    assert all(query) == ~s{SELECT x FROM schema GROUP BY x}

    query = Schema |> group_by([r], 2) |> select([r], r.x) |> plan()
    assert all(query) == ~s{SELECT x FROM schema GROUP BY 2}

    query = Schema |> group_by([r], [r.x, r.y]) |> select([r], r.x) |> plan()
    assert all(query) == ~s{SELECT x FROM schema GROUP BY x, y}

    query = Schema |> group_by([r], []) |> select([r], r.x) |> plan()
    assert all(query) == ~s{SELECT x FROM schema}
  end

  test "fragments allow ? to be escaped with backslash" do
    query =
      plan(
        from(e in "schema",
          where: fragment("? = \"query\\?\"", e.start_time),
          select: true
        )
      )

    result = "SELECT TRUE FROM schema WHERE start_time = \"query?\""

    assert all(query) == String.trim(result)
  end

  test "build_explain_query" do
    assert_raise RuntimeError, fn ->
      SQL.build_explain_query("SELECT 1", [])
    end
  end

  ## *_all

  test "update all" do
    query = from(m in Schema, update: [set: [x: 0]]) |> plan(:update_all)

    assert update_all(query) == ~s{UPDATE schema SET x = 0}

    query = from(m in Schema, update: [set: [x: 0, y: 1], set: [z: 2]]) |> plan(:update_all)

    assert update_all(query) == ~s{UPDATE schema SET x = 0, y = 1, z = 2}
  end

  test "update all with prefix" do
    query =
      from(m in Schema, update: [set: [x: 0]])
      |> Ecto.Query.put_query_prefix("prefix")
      |> plan(:update_all)

    assert update_all(query) == ~s{UPDATE prefix.schema SET x = 0}
  end

  test "delete all" do
    query = Schema |> Queryable.to_query() |> plan()
    assert delete_all(query) == ~s{DELETE FROM schema}

    query = from(e in Schema, where: e.x == 123) |> plan()

    assert delete_all(query) ==
             ~s{DELETE FROM schema WHERE x = 123}
  end

  test "delete all with prefix" do
    query = Schema |> Ecto.Query.put_query_prefix("prefix") |> plan()
    assert delete_all(query) == ~s{DELETE FROM prefix.schema}

    query = Schema |> from(prefix: "first") |> Ecto.Query.put_query_prefix("prefix") |> plan()
    assert delete_all(query) == ~s{DELETE FROM first.schema}
  end

  ## Partitions and windows

  describe "windows" do
    test "one window" do
      query =
        Schema
        |> select([r], r.x)
        |> windows([r], w: [partition_by: r.x])
        |> plan

      assert_raise Ecto.QueryError, ~r"window is not supported by Exandra", fn ->
        all(query)
      end
    end
  end

  ## Joins

  test "join without schema" do
    query =
      "posts" |> join(:inner, [p], q in "comments", on: p.x == q.z) |> select([], true) |> plan()

    assert_raise Ecto.QueryError, ~r"join is not supported by Exandra", fn ->
      all(query)
    end
  end

  # Schema based

  test "insert" do
    query = insert(nil, "schema", [:x, :y], [[:x, :y]], {:raise, [], []}, [])
    assert query == ~s{INSERT INTO schema (x, y) VALUES (?, ?) }

    query = insert(nil, "schema", [], [[]], {:raise, [], []}, [])
    assert query == ~s{INSERT INTO schema () VALUES () }

    query = insert("prefix", "schema", [], [[]], {:raise, [], []}, [])
    assert query == ~s{INSERT INTO prefix.schema () VALUES () }
  end

  test "update" do
    query = update(nil, "schema", [:id], [x: 1, y: 2], [])
    assert query == ~s{UPDATE schema SET id = ? WHERE x = ? AND y = ? }

    query = update("prefix", "schema", [:id], [x: 1, y: 2], [], allow_insert: false)
    assert query == ~s{UPDATE prefix.schema SET id = ? WHERE x = ? AND y = ?  IF EXISTS}
  end

  test "delete" do
    query = delete(nil, "schema", [x: 1, y: 2], [])
    assert query == ~s{DELETE FROM schema WHERE x = ? AND y = ?}

    query = delete("prefix", "schema", [x: 1, y: 2], [])
    assert query == ~s{DELETE FROM prefix.schema WHERE x = ? AND y = ?}
  end

  test "table_exists_query/1" do
    assert {sql, params} = SQL.table_exists_query("my_table")
    assert sql == "SELECT table_name FROM system_schema.tables WHERE table_name = ?"
    assert params == ["my_table"]
  end

  # DDL

  import Ecto.Migration, only: [table: 1, table: 2, index: 2, index: 3, constraint: 3]

  test "executing a string during migration" do
    assert execute_ddl("example") == ["example"]
  end

  test "create table" do
    create =
      {:create, table(:posts),
       [
         {:add, :id, :binary_id, [primary_key: true]},
         {:add, :name, :string, []},
         {:add, :token, :binary, []},
         {:add, :price, :decimal, []},
         {:add, :on_hand, :integer, []},
         {:add, :likes, :bigint, []},
         {:add, :published_at, :utc_datetime, []},
         {:add, :is_active, :boolean, []}
       ]}

    assert execute_ddl(create) == [
             """
             CREATE TABLE posts (id uuid,
             name text,
             token blob,
             price decimal,
             on_hand int,
             likes bigint,
             published_at timestamp,
             is_active boolean,
             PRIMARY KEY (id))
             """
             |> remove_newlines
           ]
  end

  test "create empty table raises an exception" do
    create = {:create, table(:posts), []}

    assert_raise RuntimeError, "you must define at least one column", fn ->
      execute_ddl(create)
    end

    alter = {:alter, table(:posts), []}

    assert_raise RuntimeError, "you must define at least one column", fn ->
      execute_ddl(alter)
    end
  end

  test "create table with reference raises an exception" do
    create =
      {:create, table(:posts, prefix: :foo),
       [{:add, :category_0, %Reference{table: :categories}, []}]}

    assert_raise ArgumentError, ~r/illegal column :category_0 of type references/, fn ->
      execute_ddl(create)
    end
  end

  test "create table with UDT freezes column" do
    create =
      {:create, table(:posts),
       [
         {:add, :id, :uuid, [primary_key: true]},
         {:add, :created_at, :datetime, []},
         {:add, :name, Exandra.UDT, [type: :fullname]}
       ]}

    assert execute_ddl(create) ==
             [
               ~s|CREATE TABLE posts (id uuid, created_at timestamp, name FROZEN<fullname>, PRIMARY KEY (id))|
             ]
  end

  test "create table with counter" do
    create =
      {:create, table(:post_likes),
       [
         {:add, :region_id, :uuid, [primary_key: true]},
         {:add, :post_id, :uuid, [primary_key: true]},
         {:add, :total, :counter, []}
       ]}

    assert execute_ddl(create) ==
             [
               ~s|CREATE TABLE post_likes (region_id uuid, post_id uuid, total counter, PRIMARY KEY (region_id, post_id))|
             ]
  end

  test "create table UDT without type option raises" do
    create =
      {:create, table(:posts),
       [
         {:add, :id, :uuid, [primary_key: true]},
         {:add, :created_at, :datetime, []},
         {:add, :name, Exandra.UDT, []}
       ]}

    assert_raise NimbleOptions.ValidationError, ~r/required :type option not found/, fn ->
      execute_ddl(create)
    end
  end

  test "create table with various options" do
    create =
      {:create, table(:posts, options: [caching: %{enabled: true}]),
       [{:add, :id, :uuid, [primary_key: true]}]}

    assert execute_ddl(create) ==
             [
               ~s|CREATE TABLE posts (id uuid, PRIMARY KEY (id)) WITH caching = {'enabled': 'true'}|
             ]

    create =
      {:create, table(:posts, options: [caching: %{keys: "NONE", rows_per_partition: 120}]),
       [{:add, :id, :uuid, [primary_key: true]}]}

    assert execute_ddl(create) ==
             [
               ~s|CREATE TABLE posts (id uuid, PRIMARY KEY (id)) WITH caching = {'keys': 'NONE', 'rows_per_partition': '120'}|
             ]

    create =
      {:create,
       table(:posts,
         options: [
           caching: %{keys: "NONE", rows_per_partition: 120},
           compactions: %{class: "SizeTieredCompantionStrategy", min_threshold: 4}
         ]
       ), [{:add, :id, :uuid, [primary_key: true]}]}

    assert execute_ddl(create) ==
             [
               ~s|CREATE TABLE posts (id uuid, PRIMARY KEY (id)) WITH caching = {'keys': 'NONE', 'rows_per_partition': '120'} AND compactions = {'class': 'SizeTieredCompantionStrategy', 'min_threshold': '4'}|
             ]
  end

  test "create table with string opts" do
    create =
      {:create, table(:posts, options: "WITH FOO=BAR"),
       [{:add, :id, :uuid, [primary_key: true]}, {:add, :created_at, :datetime, []}]}

    assert execute_ddl(create) ==
             [
               ~s|CREATE TABLE posts (id uuid, created_at timestamp, PRIMARY KEY (id)) WITH FOO=BAR|
             ]
  end

  test "create table with composite key" do
    create =
      {:create, table(:posts),
       [
         {:add, :a, :integer, [primary_key: true]},
         {:add, :b, :integer, [primary_key: true]},
         {:add, :name, :string, []}
       ]}

    assert execute_ddl(create) == [
             """
             CREATE TABLE posts (a int, b int, name text, PRIMARY KEY (a, b))
             """
             |> remove_newlines
           ]
  end

  test "create table with no primary/partition or clustering key raises an error" do
    create =
      {:create, table(:posts),
       [
         {:add, :a, :map, []}
       ]}

    assert_raise ArgumentError,
                 "you must define at least one primary, or clustering key",
                 fn ->
                   execute_ddl(create)
                 end
  end

  describe "primary/cluster/partition keys" do
    test "supports simple primary key" do
      create =
        {:create, table(:cycling),
         [
           {:add, :race_year, :integer, [primary_key: true]},
           {:add, :race_name, :text, []},
           {:add, :cyclist_name, :text, []},
           {:add, :rank, :integer, []}
         ]}

      assert execute_ddl(create) == [
               """
               CREATE TABLE cycling
               (race_year int,
               race_name text,
               cyclist_name text,
               rank int,
               PRIMARY KEY (race_year))
               """
               |> remove_newlines
             ]
    end

    test "supports composite primary key" do
      create =
        {:create, table(:cycling),
         [
           {:add, :race_year, :integer, [primary_key: true]},
           {:add, :race_name, :text, [primary_key: true]},
           {:add, :cyclist_name, :text, []},
           {:add, :rank, :integer, [cluster_key: true]}
         ]}

      assert execute_ddl(create) == [
               """
               CREATE TABLE cycling
               (race_year int,
               race_name text,
               cyclist_name text,
               rank int,
               PRIMARY KEY ((race_year, race_name), rank))
               """
               |> remove_newlines
             ]
    end

    test "deprecated partition_key option still works" do
      create =
        {:create, table(:cycling),
         [
           {:add, :race_year, :integer, [primary_key: true]},
           {:add, :race_name, :text, [primary_key: true]},
           {:add, :cyclist_name, :text, []},
           {:add, :rank, :integer, [partition_key: true]}
         ]}

      assert execute_ddl(create) == [
               """
               CREATE TABLE cycling
               (race_year int,
               race_name text,
               cyclist_name text,
               rank int,
               PRIMARY KEY ((race_year, race_name), rank))
               """
               |> remove_newlines
             ]
    end

    test "supports composite primary key with ordering in PK definition" do
      create =
        {:create, table(:cycling),
         [
           {:add, :race_year, :integer, [primary_key: true, primary_key_order: 1]},
           {:add, :race_name, :text, [primary_key: true, primary_key_order: 0]},
           {:add, :cyclist_name, :text, []},
           {:add, :rank, :integer, [cluster_key: true]}
         ]}

      assert execute_ddl(create) == [
               """
               CREATE TABLE cycling
               (race_year int,
               race_name text,
               cyclist_name text,
               rank int,
               PRIMARY KEY ((race_name, race_year), rank))
               """
               |> remove_newlines
             ]
    end

    test "supports composite primary key with ordering in parition definition" do
      create =
        {:create, table(:cycling),
         [
           {:add, :race_year, :integer, [primary_key: true, primary_key_order: 1]},
           {:add, :race_name, :text, [primary_key: true, primary_key_order: 0]},
           {:add, :cyclist_name, :text, []},
           {:add, :rank, :integer, [cluster_key: true, cluster_key_order: 1]},
           {:add, :region, :text, [cluster_key: true, cluster_key_order: 0]}
         ]}

      assert execute_ddl(create) == [
               """
               CREATE TABLE cycling
               (race_year int,
               race_name text,
               cyclist_name text,
               rank int,
               region text,
               PRIMARY KEY ((race_name, race_year), region, rank))
               """
               |> remove_newlines
             ]
    end

    test "supports deprecated partition key with ordering in parition definition" do
      create =
        {:create, table(:cycling),
         [
           {:add, :race_year, :integer, [primary_key: true, primary_key_order: 1]},
           {:add, :race_name, :text, [primary_key: true, primary_key_order: 0]},
           {:add, :cyclist_name, :text, []},
           {:add, :rank, :integer, [partition_key: true, partition_key_order: 1]},
           {:add, :region, :text, [partition_key: true, partition_key_order: 0]}
         ]}

      assert execute_ddl(create) == [
               """
               CREATE TABLE cycling
               (race_year int,
               race_name text,
               cyclist_name text,
               rank int,
               region text,
               PRIMARY KEY ((race_name, race_year), region, rank))
               """
               |> remove_newlines
             ]
    end

    test "support clustering order by" do
      create =
        {:create, table(:cycling),
         [
           {:add, :race_year, :integer,
            [primary_key: true, primary_key_order: 1, cluster_ordering: :desc]},
           {:add, :race_name, :text, [primary_key: true, primary_key_order: 0]},
           {:add, :cyclist_name, :text, []}
         ]}

      assert execute_ddl(create) == [
               """
               CREATE TABLE cycling
               (race_year int,
               race_name text,
               cyclist_name text,
               PRIMARY KEY (race_name, race_year)) WITH CLUSTERING ORDER BY (race_year DESC)
               """
               |> remove_newlines
             ]

      create =
        {:create, table(:cycling),
         [
           {:add, :race_year, :integer,
            [
              primary_key: true,
              primary_key_order: 1,
              cluster_ordering: :desc,
              cluster_key_order: 1
            ]},
           {:add, :race_name, :text,
            [
              primary_key: true,
              primary_key_order: 0,
              cluster_ordering: :asc,
              cluster_key_order: 0
            ]},
           {:add, :cyclist_name, :text, []}
         ]}

      assert execute_ddl(create) == [
               """
               CREATE TABLE cycling
               (race_year int,
               race_name text,
               cyclist_name text,
               PRIMARY KEY (race_name, race_year)) WITH CLUSTERING ORDER BY (race_name ASC, race_year DESC)
               """
               |> remove_newlines
             ]
    end
  end

  test "create table with time columns" do
    create =
      {:create, table(:posts),
       [{:add, :published_at, :time, [primary_key: true]}, {:add, :submitted_at, :time, []}]}

    assert execute_ddl(create) == [
             """
             CREATE TABLE posts
             (published_at time,
             submitted_at time, PRIMARY KEY (published_at))
             """
             |> remove_newlines
           ]
  end

  test "create table with utc_datetime columns" do
    create =
      {:create, table(:posts),
       [
         {:add, :published_at, :utc_datetime, [precision: 3]},
         {:add, :submitted_at, :utc_datetime, [primary_key: true]}
       ]}

    assert execute_ddl(create) == [
             """
             CREATE TABLE posts
             (published_at timestamp,
             submitted_at timestamp, PRIMARY KEY (submitted_at))
             """
             |> remove_newlines
           ]
  end

  test "create table with naive_datetime columns" do
    create =
      {:create, table(:posts),
       [
         {:add, :published_at, :naive_datetime, [precision: 3, cluster_key: true]},
         {:add, :submitted_at, :naive_datetime, [primary_key: true]}
       ]}

    assert execute_ddl(create) == [
             """
             CREATE TABLE posts
             (published_at timestamp,
             submitted_at timestamp, PRIMARY KEY ((submitted_at), published_at))
             """
             |> remove_newlines
           ]
  end

  test "create table with an unsupported type" do
    create =
      {:create, table(:posts),
       [
         {:add, :a, {:a, :b, :c}, [default: %{}]}
       ]}

    assert_raise ArgumentError, ~r/unsupported type \{:a, :b, :c\} for column :a/, fn ->
      execute_ddl(create)
    end
  end

  test "drop table" do
    drop = {:drop, table(:posts), :restrict}
    assert execute_ddl(drop) == [~s|DROP TABLE posts|]
  end

  test "drop table with prefixes" do
    drop = {:drop, table(:posts, prefix: :foo), :restrict}
    assert execute_ddl(drop) == [~s|DROP TABLE foo.posts|]
  end

  test "drop constraint" do
    assert_raise ArgumentError, ~r/constraints are not supported by Exandra/, fn ->
      execute_ddl(
        {:drop, constraint(:products, "price_must_be_positive", prefix: :foo), :restrict}
      )
    end
  end

  test "drop_if_exists constraint" do
    assert_raise ArgumentError, ~r/constraints are not supported by Exandra/, fn ->
      execute_ddl(
        {:drop_if_exists, constraint(:products, "price_must_be_positive", prefix: :foo),
         :restrict}
      )
    end
  end

  test "alter table with more that one type of alter raises" do
    alter =
      {:alter, table(:posts),
       [
         {:add, :title, :string, []},
         {:modify, :price, :decimal, []},
         {:modify, :cost, :integer, []},
         {:modify, :status, :string, from: :integer},
         {:remove, :summary},
         {:remove, :body, :text, []}
       ]}

    assert_raise ArgumentError,
                 ~r"Exandra does not support more than one type of operation",
                 fn ->
                   execute_ddl(alter)
                 end
  end

  test "alter table with multiples" do
    alter =
      {:alter, table(:posts),
       [
         {:add, :title, :string, []},
         {:add, :price, :decimal, []}
       ]}

    assert execute_ddl(alter) == [
             """
             ALTER TABLE posts ADD (title text, price decimal)
             """
             |> remove_newlines
           ]

    alter =
      {:alter, table(:posts),
       [
         {:remove, :title},
         {:remove, :price}
       ]}

    assert execute_ddl(alter) == [
             """
             ALTER TABLE posts DROP (title, price)
             """
             |> remove_newlines
           ]
  end

  test "alter table with single operations" do
    alter =
      {:alter, table(:posts),
       [
         {:add, :title, :string, []}
       ]}

    assert execute_ddl(alter) == [
             """
             ALTER TABLE posts ADD title text
             """
             |> remove_newlines
           ]

    alter =
      {:alter, table(:posts),
       [
         {:remove, :title}
       ]}

    assert execute_ddl(alter) == [
             """
             ALTER TABLE posts DROP title
             """
             |> remove_newlines
           ]

    alter =
      {:alter, table(:posts),
       [
         {:modify, :price, :decimal, []}
       ]}

    assert execute_ddl(alter) == [
             """
             ALTER TABLE posts ALTER price TYPE decimal
             """
             |> remove_newlines
           ]
  end

  test "alter table with prefix" do
    alter =
      {:alter, table(:posts, prefix: :foo),
       [
         {:add, :author_id, :uuid, []},
         {:add, :permalink_id, :string, []}
       ]}

    assert execute_ddl(alter) == [
             """
             ALTER TABLE foo.posts ADD (author_id uuid, permalink_id text)
             """
             |> remove_newlines
           ]
  end

  test "alter table with primary key" do
    alter =
      {:alter, table(:posts), [{:add, :my_pk, :uuid, [primary_key: true, clustering_key: true]}]}

    assert_raise ArgumentError, "altering PRIMARY KEY columns is not supported", fn ->
      execute_ddl(alter)
    end
  end

  test "create constraints" do
    assert_raise ArgumentError, "constraints are not supported by Exandra", fn ->
      create = {:create, constraint(:products, "foo", check: "price")}
      assert execute_ddl(create)
    end
  end

  test "create index" do
    create = {:create, index(:posts, [:permalink])}

    assert execute_ddl(create) ==
             [
               ~s|CREATE INDEX posts_permalink_index ON posts(permalink)|
             ]

    create = {:create, index(:posts, [:permalink], name: "posts$main")}

    assert execute_ddl(create) ==
             [~s|CREATE INDEX posts$main ON posts(permalink)|]
  end

  test "create index with bad options raises" do
    create = {:create, index(:posts, [:category_id], prefix: :foo)}

    assert_raise ArgumentError, "prefix index creation is not supported by Exandra", fn ->
      execute_ddl(create)
    end

    create = {:create, index(:posts, [:category_id], unique: true)}

    assert_raise ArgumentError, "unique index creation is not supported by Exandra", fn ->
      execute_ddl(create)
    end

    create = {:create, index(:posts, [:category_id], include: [:public])}

    assert_raise ArgumentError, "include index creation is not supported by Exandra", fn ->
      execute_ddl(create)
    end

    create = {:create, index(:posts, [:category_id], where: "1=1")}

    assert_raise ArgumentError, "where index creation is not supported by Exandra", fn ->
      execute_ddl(create)
    end

    index = index(:posts, [:permalink])
    create = {:create, %{index | concurrently: true}}

    assert_raise ArgumentError, "concurrent index creation is not supported by Exandra", fn ->
      execute_ddl(create)
    end
  end

  test "drop index" do
    drop = {:drop, index(:posts, [:id], name: "posts$main"), :restrict}
    assert execute_ddl(drop) == [~s|DROP INDEX posts$main|]

    drop = {:drop_if_exists, index(:posts, [:id]), :restrict}
    assert execute_ddl(drop) == [~s|DROP INDEX IF EXISTS posts_id_index|]

    drop = {:drop_if_exists, index(:posts, [:id]), :restrict}
    assert execute_ddl(drop) == [~s|DROP INDEX IF EXISTS posts_id_index|]
  end

  test "drop index with bad options raises" do
    drop = {:drop, index(:posts, [:id]), :cascade}

    assert_raise ArgumentError, "cascade index drop is not supported by Exandra", fn ->
      execute_ddl(drop)
    end

    drop = {:drop, index(:posts, [:id], prefix: :foo), :restrict}

    assert_raise ArgumentError, "prefix index drop is not supported by Exandra", fn ->
      execute_ddl(drop)
    end
  end

  test "rename column" do
    rename = {:rename, table(:posts), :title, :title2}
    assert execute_ddl(rename) == [~s|ALTER TABLE posts RENAME title TO title2|]
  end

  # Unsupported types and clauses

  test "arrays" do
    assert_raise Ecto.QueryError, ~r"Array type is not supported by Exandra", fn ->
      query = Schema |> select([], fragment("?", [1, 2, 3])) |> plan()
      all(query)
    end
  end

  defp remove_newlines(string) do
    string |> String.trim() |> String.replace("\n", " ")
  end
end
