defmodule Exandra.Connection do
  @behaviour Ecto.Adapters.SQL.Connection

  alias Ecto.Migration.{Constraint, Index, Reference, Table}
  alias Ecto.Query.{BooleanExpr, QueryExpr, WithExpr}
  alias Exandra.{Adapter, Types}
  alias Xandra.Prepared

  def build_explain_query(_, _) do
    raise RuntimeError, "not supported"
  end

  @impl Ecto.Adapters.SQL.Connection
  def child_spec(opts) do
    Adapter.child_spec(opts)
  end

  def in_transaction?(%{sql: Exandra.Connection}), do: true

  @impl Ecto.Adapters.SQL.Connection
  def prepare_execute(cluster, _name, stmt, params, opts) do
    with {:ok, %Prepared{} = prepared} <- Adapter.prepare(cluster, stmt, opts) do
      execute(cluster, prepared, params, opts)
    end
  end

  @impl Ecto.Adapters.SQL.Connection
  def execute(cluster, query, params, opts) do
    values = Enum.map(params, fn {_, value} -> value end)
    stream = Adapter.stream_pages!(cluster, query, values, opts)

    result =
      Enum.reduce_while(stream, %{rows: [], num_rows: 0}, fn
        %Xandra.Void{}, _acc ->
          {:halt, %{rows: nil, num_rows: 1}}

        %Xandra.SchemaChange{}, _acc ->
          {:halt, %{rows: nil, num_rows: 1}}

        %Xandra.Page{} = page, %{rows: rows, num_rows: num_rows} ->
          %{rows: new_rows, num_rows: new_num_rows} = process_page(page)
          {:cont, %{rows: rows ++ new_rows, num_rows: num_rows + new_num_rows}}
      end)

    {:ok, query, result}
  rescue
    err ->
      {:error, err}
  end

  @impl Ecto.Adapters.SQL.Connection
  def query(cluster, sql, params, opts) do
    case Adapter.execute(cluster, sql, params, opts) do
      {:ok, %Xandra.SchemaChange{} = schema_change} ->
        {:ok, schema_change}

      {:ok, %Xandra.Void{}} ->
        {:ok, %{rows: nil, num_rows: 1}}

      {:ok, %Xandra.Page{paging_state: nil} = page} ->
        {:ok, process_page(page)}

      {:error, _} = err ->
        err
    end
  end

  @impl Ecto.Adapters.SQL.Connection
  def query_many(_cluster, _sql, _params, _opts) do
    raise RuntimeError, "query_many is not supported in Exandra"
  end

  @impl Ecto.Adapters.SQL.Connection
  def stream(_, _, _, _), do: raise("not implemented")

  @impl Ecto.Adapters.SQL.Connection
  def explain_query(_, _, _, _), do: raise("not implemented")

  @impl Ecto.Adapters.SQL.Connection
  def all(query, as_prefix \\ []) do
    sources = create_names(query, as_prefix)

    cte = cte(query, sources)
    from = from(query, sources)
    select = select(query, sources)
    join = join(query, sources)
    where = where(query, sources)
    group_by = group_by(query, sources)
    having = having(query, sources)
    window = window(query, sources)
    combinations = combinations(query)
    order_by = order_by(query, sources)
    limit = limit(query, sources)
    offset = offset(query, sources)
    lock = lock(query, sources)

    [
      cte,
      select,
      from,
      join,
      where,
      group_by,
      having,
      window,
      combinations,
      order_by,
      limit,
      offset | lock
    ]
  end

  @impl Ecto.Adapters.SQL.Connection
  def insert(prefix, table, headers, rows, _on_conflict, _returning, opts)
      when prefix in [nil, ""] do
    keys = Enum.join(headers, ", ")
    values = Enum.map(rows, &Enum.map_join(&1, ", ", fn _ -> "?" end))

    "INSERT INTO #{quote_table(prefix, table)} (#{keys}) VALUES (#{values}) #{insert_suffix(opts)}"
  end

  @impl Ecto.Adapters.SQL.Connection
  def update(prefix, table, fields, filters, _returning) do
    "UPDATE #{quote_table(prefix, table)} SET #{set(fields)} WHERE #{where(filters)}"
  end

  @impl Ecto.Adapters.SQL.Connection
  def update_all(_), do: raise("not implemented")

  @impl Ecto.Adapters.SQL.Connection
  def delete(prefix, table, filters, _returning) do
    "DELETE FROM #{quote_table(prefix, table)} WHERE #{where(filters)}"
  end

  @impl Ecto.Adapters.SQL.Connection
  def delete_all(query) do
    sources = create_names(query, [])

    cte(query, sources)
    combinations(query)

    from = from(query, sources)
    where = where(query, sources)
    ["DELETE", from, where]
  end

  defp distinct(nil, _sources, _query), do: []
  defp distinct(%QueryExpr{expr: true}, _sources, _query), do: "DISTINCT "
  defp distinct(%QueryExpr{expr: false}, _sources, _query), do: []

  defp distinct(%QueryExpr{expr: exprs}, _sources, query) when is_list(exprs) do
    error!(query, "DISTINCT with multiple columns is not supported by MySQL")
  end

  defp select(%{select: %{fields: fields}, distinct: distinct} = query, sources) do
    ["SELECT ", distinct(distinct, sources, query) | select(fields, sources, query)]
  end

  defp select([], _sources, _query),
    do: "TRUE"

  defp select(fields, sources, query) do
    intersperse_map(fields, ", ", fn
      {:&, _, [idx]} ->
        case elem(sources, idx) do
          {source, _, nil} ->
            error!(
              query,
              "Scylla does not support selecting all fields from #{source} without a schema. " <>
                "Please specify a schema or specify exactly which fields you want to select"
            )

          {_, source, _} ->
            source
        end

      {key, value} ->
        [expr(value, sources, query), " AS ", quote_name(key)]

      value ->
        expr(value, sources, query)
    end)
  end

  defp from(%{from: %{source: {from, _schema}, hints: hints}}, _sources) do
    [" FROM ", from, Enum.map(hints, &[?\s | &1])]
  end

  defp from(query, _) do
    error!(
      query,
      "Scylla Adapter does not support subqueries at this time."
    )
  end

  defp cte(
         %{with_ctes: %WithExpr{recursive: _recursive, queries: [_ | _] = _queries}} = query,
         _sources
       ) do
    error!(
      query,
      "Scylla Adapter does not support cte at this time."
    )
  end

  defp cte(_, _), do: []

  defp lock(%{lock: nil}, _sources), do: []

  defp lock(%{lock: _expr} = query, _sources),
    do: error!(query, "`lock` is not supported by Exandra")

  defp window(%{windows: []}, _sources), do: []
  defp window(query, _sources), do: error!(query, "window is not supported by Exandra")

  defp join(%{joins: []}, _sources), do: []
  defp join(query, _sources), do: error!(query, "join is not supported by Exandra")

  defp limit(%{limit: nil}, _sources), do: []

  defp limit(%{limit: %QueryExpr{expr: expr}} = query, sources) do
    [" LIMIT " | expr(expr, sources, query)]
  end

  defp offset(%{offset: nil}, _sources), do: []

  defp offset(%{offset: %QueryExpr{expr: _}} = query, _sources) do
    error!(query, "`offset` is not supported by Exandra")
  end

  defp order_by(%{order_bys: []}, _sources), do: []

  defp order_by(%{order_bys: order_bys} = query, sources) do
    [
      " ORDER BY "
      | intersperse_map(order_bys, ", ", fn %QueryExpr{expr: expr} ->
          intersperse_map(expr, ", ", &order_by_expr(&1, sources, query))
        end)
    ]
  end

  defp set(fields), do: Enum.map_join(fields, ", ", &"#{&1} = ?")

  @impl Ecto.Adapters.SQL.Connection
  def to_constraints(_, _), do: []

  @impl Ecto.Adapters.SQL.Connection
  def table_exists_query(table),
    do: {"SELECT table_name FROM system_schema.tables WHERE table_name = '#{table}'", []}

  defp insert_suffix(opts) do
    suffix =
      case Keyword.get(opts, :overwrite, true) do
        true ->
          []

        _ ->
          [" IF NOT EXISTS"]
      end

    suffix =
      case Keyword.get(opts, :ttl, nil) do
        nil -> suffix
        seconds when is_number(seconds) -> suffix ++ [" USING TTL #{seconds}"]
      end

    case Keyword.get(opts, :timestamp, nil) do
      nil ->
        suffix

      epoch_in_microseconds when is_number(epoch_in_microseconds) ->
        suffix ++ [" AND TIMESTAMP #{epoch_in_microseconds}"]
    end
  end

  defp boolean(_name, [], _sources, _query), do: []

  defp boolean(name, [%{expr: expr, op: op} | query_exprs], sources, query) do
    [
      name,
      query_exprs
      |> Enum.reduce({op, paren_expr(expr, sources, query)}, fn
        %BooleanExpr{expr: expr, op: op}, {op, acc} ->
          {op, [acc, operator_to_boolean(op) | paren_expr(expr, sources, query)]}

        %BooleanExpr{expr: expr, op: op}, {_, acc} ->
          {op, [?(, acc, ?), operator_to_boolean(op) | paren_expr(expr, sources, query)]}
      end)
      |> elem(1)
    ]
  end

  defp combinations(%{combinations: combinations}) do
    Enum.map(combinations, fn
      {union_type, query} -> error!(query, "`#{union_type}` is not supported by Exandra")
    end)
  end

  defp order_by_expr({dir, expr}, sources, query) do
    str = expr(expr, sources, query)

    case dir do
      :asc -> [str | " ASC"]
      :desc -> [str | " DESC"]
      _ -> error!(query, "#{dir} is not supported in ORDER BY in Exandra")
    end
  end

  defp group_by(%{group_bys: []}, _sources), do: []

  defp group_by(%{group_bys: group_bys} = query, sources) do
    [
      " GROUP BY "
      | intersperse_map(group_bys, ", ", fn %QueryExpr{expr: expr} ->
          intersperse_map(expr, ", ", &expr(&1, sources, query))
        end)
    ]
  end

  defp where(%{wheres: wheres} = query, sources) do
    boolean(" WHERE ", wheres, sources, query)
  end

  defp where(filters) when is_list(filters) do
    Enum.map_join(filters, " AND ", fn {k, _} -> "#{k} = ?" end)
  end

  defp having(%{havings: []}, _sources), do: []

  defp having(%{havings: _} = query, _sources) do
    error!(query, "HAVING is not supported by Exandra")
  end

  ## Query generation helpers

  binary_ops = [
    ==: " = ",
    !=: " != ",
    <=: " <= ",
    >=: " >= ",
    <: " < ",
    >: " > ",
    +: " + ",
    -: " - ",
    *: " * ",
    /: " / ",
    and: " AND ",
    or: " OR ",
    like: " LIKE "
  ]

  Enum.map(binary_ops, fn {op, str} ->
    defp handle_call(unquote(op), 2), do: {:binary_op, unquote(str)}
  end)

  defp handle_call(fun, _arity), do: {:fun, Atom.to_string(fun)}

  defp paren_expr(expr, sources, query) do
    [expr(expr, sources, query)]
  end

  defp expr(%Ecto.Query.Tagged{value: other, type: type}, sources, query) do
    ["CAST(", expr(other, sources, query), " AS ", ecto_cast_to_db(type, query), ?)]
  end

  defp expr({:^, [], [_ix]}, _sources, _query) do
    '?'
  end

  defp expr({{:., _, [{:&, _, [_idx]}, field]}, _, []}, _sources, _query)
       when is_atom(field) do
    [quote_name(field)]
  end

  defp expr({:in, _, [left, {:^, _, [_, length]}]}, sources, query) do
    args = Enum.intersperse(List.duplicate(??, length), ?,)
    [expr(left, sources, query), " IN (", args, ?)]
  end

  defp expr({:is_nil, _, [arg]}, sources, query) do
    [expr(arg, sources, query) | " IS NULL"]
  end

  defp expr({:not, _, [expr]}, sources, query) do
    ["NOT (", expr(expr, sources, query), ?)]
  end

  defp expr({:fragment, _, [kw]}, _sources, query) when is_list(kw) or tuple_size(kw) == 3 do
    error!(query, "Exandra does not support keyword or interpolated fragments")
  end

  defp expr({:fragment, _, parts}, sources, query) do
    Enum.map(parts, fn
      {:raw, part} -> part
      {:expr, expr} -> expr(expr, sources, query)
    end)
  end

  defp expr({:filter, _, _}, _sources, query) do
    error!(query, "Exandra does not support aggregate filters")
  end

  defp expr({:{}, _, elems}, sources, query) do
    [?(, intersperse_map(elems, ?,, &expr(&1, sources, query)), ?)]
  end

  defp expr({:count, _, []}, _sources, _query), do: "count(*)"

  defp expr({:selected_as, _, [name]}, _sources, _query) do
    [quote_name(name)]
  end

  defp expr({fun, _, args}, sources, query) when is_atom(fun) and is_list(args) do
    {modifier, args} =
      case args do
        [rest, :distinct] -> {"DISTINCT ", [rest]}
        _ -> {[], args}
      end

    case handle_call(fun, length(args)) do
      {:binary_op, op} ->
        [left, right] = args
        [op_to_binary(left, sources, query), op | op_to_binary(right, sources, query)]

      {:fun, "coalesce"} ->
        error!(query, "COALESCE function is not supported by Exandra")

      {:fun, fun} ->
        [fun, ?(, modifier, intersperse_map(args, ", ", &expr(&1, sources, query)), ?)]
    end
  end

  defp expr(list, _sources, query) when is_list(list) do
    error!(query, "Array type is not supported by Exandra")
  end

  defp expr(%Decimal{} = decimal, _sources, _query) do
    Decimal.to_string(decimal, :normal)
  end

  defp expr(nil, _sources, _query), do: "NULL"
  defp expr({"boolean", false}, _sources, _query), do: "FALSE"
  defp expr({"boolean", true}, _sources, _query), do: "TRUE"
  defp expr({"int", val}, _sources, _query), do: "#{val}"
  defp expr({"uuid", binary_id}, _sources, _query), do:  "'" <> binary_id <> "'"
  defp expr({"text", string}, _sources, _query), do:  "'" <> string <> "'"

  defp expr(literal, _sources, _query) when is_binary(literal) do
    [?', escape_string(literal), ?']
  end

  defp expr(literal, _sources, _query) when is_integer(literal) do
    Integer.to_string(literal)
  end

  defp expr(literal, _sources, _query) when is_float(literal) do
    # Scylla doesn't support float cast
    ["(0 + ", Float.to_string(literal), ?)]
  end

  defp error!(query, message) do
    raise Ecto.QueryError, query: query, message: message
  end

  defp operator_to_boolean(:and), do: " AND "
  defp operator_to_boolean(:or), do: " OR "

  defp op_to_binary({:is_nil, _, [_]} = expr, sources, query),
    do: paren_expr(expr, sources, query)

  defp op_to_binary(expr, sources, query),
    do: expr(expr, sources, query)

  defp escape_string(value) when is_binary(value) do
    value
    |> :binary.replace("'", "''", [:global])
    |> :binary.replace("\\", "\\\\", [:global])
  end

  defp intersperse_map(list, separator, mapper, acc \\ [])

  defp intersperse_map([elem], _separator, mapper, acc),
    do: [acc | mapper.(elem)]

  defp intersperse_map([elem | rest], separator, mapper, acc),
    do: intersperse_map(rest, separator, mapper, [acc, mapper.(elem), separator])

  defp create_names(%{sources: sources}, as_prefix) do
    sources |> create_names(0, tuple_size(sources), as_prefix) |> List.to_tuple()
  end

  defp create_names(sources, pos, limit, as_prefix) when pos < limit do
    [create_name(sources, pos, as_prefix) | create_names(sources, pos + 1, limit, as_prefix)]
  end

  defp create_names(_sources, pos, pos, as_prefix) do
    [as_prefix]
  end

  defp create_name(sources, pos, as_prefix) do
    case elem(sources, pos) do
      {:fragment, _, _} ->
        {nil, as_prefix ++ [?f | Integer.to_string(pos)], nil}

      {table, schema, prefix} ->
        name = as_prefix ++ [create_alias(table) | Integer.to_string(pos)]
        {quote_table(prefix, table), name, schema}

      %Ecto.SubQuery{} ->
        {nil, as_prefix ++ [?s | Integer.to_string(pos)], nil}
    end
  end

  defp create_alias(<<first, _rest::binary>>)
       when first in ?a..?z
       when first in ?A..?Z,
       do: first

  defp create_alias(_), do: ?t

  @impl Ecto.Adapters.SQL.Connection
  def ddl_logs(_), do: []

  def table_options(opts, clustering_opts) when is_list(opts) do
    with_opts = for {key, config} <- opts, into: [], do: "#{key} = #{sorta_jsonify_opts(config)}"

    " WITH #{clustering_opts}" <> Enum.join(with_opts, " AND ")
  end

  def table_options(opts, clustering_opts) when is_bitstring(opts) do
    " " <> opts <> clustering_opts
  end

  def table_options(nil, clustering_opts) do
    clustering_opts
  end

  def sorta_jsonify_opts(opts) do
    opts = Enum.map_join(opts, ", ", fn {key, val} -> "'#{key}': '#{val}'" end)

    "{" <> opts <> "}"
  end

  @impl Ecto.Adapters.SQL.Connection
  def execute_ddl({command, %Table{} = table, columns})
      when command in [:create, :create_if_not_exists] do
    structure = column_definitions(columns) <> ", " <> key_definitions(columns)
    orderings = ordering_bys(columns)
    with_options = table_options(table.options, orderings)

    guard = if command == :create_if_not_exists, do: " IF NOT EXISTS ", else: ""

    [
      [
        "CREATE TABLE#{guard} #{quote_table(table.prefix, table.name)} (#{structure})" <>
          with_options
      ]
    ]
  end

  @impl Ecto.Adapters.SQL.Connection
  def execute_ddl({:alter, %Table{} = table, columns}) do
    structure = column_definitions(columns, _modify = true)

    [
      [
        "ALTER TABLE #{quote_table(table.prefix, table.name)} #{structure}"
      ]
    ]
  end

  @impl Ecto.Adapters.SQL.Connection
  def execute_ddl({command, %Table{} = table, _})
      when command in [:drop, :drop_if_not_exists] do
    guard = if command == :drop_if_not_exists, do: " IF NOT EXISTS ", else: ""

    [["DROP TABLE#{guard} #{quote_table(table.prefix, table.name)}"]]
  end

  @impl Ecto.Adapters.SQL.Connection
  def execute_ddl({_command, %Constraint{}, _}),
    do: raise(ArgumentError, "constraints are not supported by Exandra")

  @impl Ecto.Adapters.SQL.Connection
  def execute_ddl({_command, %Constraint{}}),
    do: raise(ArgumentError, "constraints are not supported by Exandra")

  @impl Ecto.Adapters.SQL.Connection
  def execute_ddl({_command, %Index{}, _}),
    do: raise(ArgumentError, "indexes are not supported by Exandra")

  @impl Ecto.Adapters.SQL.Connection
  def execute_ddl(string) when is_binary(string), do: [string]

  defp key_definitions(columns) do
    primary_keys = columns_with_opts(columns, :primary_key)
    partition_keys = columns_with_opts(columns, :partition_key)

    if [[], []] == [primary_keys, partition_keys] do
      raise ArgumentError, "you must define at least one primary, partition, or clustering key"
    end

    case {primary_keys, partition_keys} do
      {primary_keys, []} when primary_keys != [] ->
        "PRIMARY KEY (#{key_join(primary_keys)})"

      _ ->
        "PRIMARY KEY ((#{key_join(primary_keys)}), #{key_join(partition_keys)})"
    end
  end

  defp key_join([%{opts: opts} | _] = keys) do
    keys_join_by_name(
      cond do
        opts[:primary_key_order] ->
          Enum.sort_by(keys, fn key -> key.opts[:primary_key_order] end)

        opts[:partition_key_order] ->
          Enum.sort_by(keys, fn key -> key.opts[:partition_key_order] end)

        true ->
          keys
      end
    )
  end

  defp key_join([]), do: ""

  defp keys_join_by_name(keys), do: Enum.map_join(keys, ", ", fn key -> key.name end)

  defp columns_with_opts(columns, key) do
    columns
    |> Enum.filter(fn {_, _, _, opts} -> opts[key] end)
    |> Enum.map(fn {_, name, _, opts} -> %{name: name, opts: opts} end)
  end

  def column_definitions([]), do: raise(RuntimeError, "you must define at least one column")

  def column_definitions(columns, alter \\ false) do
    Enum.map_join(columns, ", ", &column_definition(&1, alter))
  end

  defp column_definition({:add, name, type, opts}, true) do
    if Keyword.has_key?(opts, :primary_key) do
      raise ArgumentError, "altering PRIMARY KEY columns is not supported"
    else
      "ADD #{quote_name(name)} #{Types.for(type, opts)}"
    end
  end

  defp column_definition({_op, name, %Reference{}, _opts}, _) do
    raise RuntimeError, "Illegal reference `#{name}` Exandra does not support associations"
  end

  defp column_definition({:add, name, type, opts}, alter) do
    prefix = if alter, do: "ADD ", else: ""
    prefix <> "#{quote_name(name)} #{Types.for(type, opts)}"
  rescue
    err in ArgumentError ->
      reraise err, __STACKTRACE__

    _err ->
      raise ArgumentError,
            "unsupported type `#{inspect(type)}`. " <>
              "The type can either be an atom, a string or a tuple of the form " <>
              "`{:map, t}` where `t` itself follows the same conditions."
  end

  defp column_definition({:modify, name, type, opts}, _) do
    "ALTER #{quote_name(name)} TYPE #{Types.for(type, opts)}"
  end

  defp column_definition({:remove, name, _type, _opts}, _) do
    "DROP #{quote_name(name)}"
  end

  defp column_definition({:remove, name}, _) do
    "DROP #{quote_name(name)}"
  end

  defp quote_name(name) when is_atom(name), do: quote_name(Atom.to_string(name))
  defp quote_name(name), do: [name]
  defp quote_table(nil, name), do: quote_table(name)
  defp quote_table(prefix, name), do: [quote_table(prefix), ?., quote_table(name)]
  defp quote_table(name) when is_atom(name), do: quote_table(Atom.to_string(name))
  defp quote_table(name), do: [name]

  defp ordering_bys(columns) do
    columns
    |> columns_with_opts(:cluster_ordering)
    |> cluster_key_sort()
    |> Enum.map(&"#{&1.name} #{ordering_by(&1.opts[:cluster_ordering])}")
    |> case do
      [] ->
        ""

      orderings ->
        " WITH CLUSTERING ORDER BY (#{Enum.join(orderings, ", ")})"
    end
  end

  def cluster_key_sort([%{opts: opts} | _] = cols) do
    if opts[:cluster_key_order] do
      Enum.sort_by(cols, fn key -> key.opts[:cluster_key_order] end)
    else
      cols
    end
  end

  def cluster_key_sort([]), do: []

  defp ordering_by(:asc), do: "ASC"
  defp ordering_by(:desc), do: "DESC"

  defp process_page(%Xandra.Page{columns: [{_, _, "[applied]", _} | _], content: content}) do
    rows =
      content
      |> Enum.reject(&match?([false | _], &1))
      |> Enum.map(fn [_ | row] -> row end)

    %{rows: rows, num_rows: length(rows)}
  end

  defp process_page(%Xandra.Page{
         columns: [{_, _, "system.count" <> _, _} | _],
         content: [[count]]
       }) do
    %{rows: [[count]], num_rows: 1}
  end

  defp process_page(%Xandra.Page{columns: [{_, _, "count" <> _, _} | _], content: [[count]]}) do
    %{rows: [[count]], num_rows: 1}
  end

  defp process_page(%Xandra.Page{content: content}) do
    %{rows: content, num_rows: length(content)}
  end

  defp ecto_cast_to_db(:binary_id, _query), do: "uuid"
  defp ecto_cast_to_db(:decimal, _query), do: "decimal"
  defp ecto_cast_to_db(:id, _query), do: "uuid"
  defp ecto_cast_to_db(:string, _query), do: "text"
  defp ecto_cast_to_db(:uuid, _query), do: "uuid"
end
