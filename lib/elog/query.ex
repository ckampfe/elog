defmodule Elog.Query do
  # names:
  # squirrel
  # squeel

  require MapSet
  require Strand.Protocol.Graph, as: Graph
  require Strand.Protocol.Digraph, as: DG
  require Strand.Impl.Digraph, as: Digraph

  def query(db, q) do
    relations =
      q
      |> validate()
      |> to_relations(db)
  end

  def validate(%{find: find, where: where} = q)
      when is_map(q) and is_list(find) and is_list(where) do
    :ok
  end

  @doc """
      iex> import Elog.Query
      iex> query = %{where: [[~q(e), :name, ~q(name)], [~q(e2), :name, ~q(name2)], [~q(e), :friend, ~q(e2)]]}
      iex> db = [%Elog.Datom{e: 1, a: :name, v: "Bill", t: 18141}, %Elog.Datom{e: 2, a: :name, v: "Sandy", t: 22222}, %Elog.Datom{e: 2, a: :friend, v: 1, t: 22222}, %Elog.Datom{e: 3, a: :name, v: "Jim should not appear", t: 33333}]
      iex> to_relations(query, db)
      :ok


      [%{symbols: %{{:var, :e2} => :e, {:var, :name2} => :v},
      tuples: [%Elog.Datom{a: :name, e: 1, t: 18141, v: "Bill"}, %Elog.Datom{a: :name, e: 2, t: 22222, v: "Sandy"}]},
      %{symbols: %{{:var, :e} => :e, {:var, :name} => :v},
      tuples: [%Elog.Datom{a: :name, e: 1, t: 18141, v: "Bill"}, %Elog.Datom{a: :name, e: 2, t: 22222, v: "Sandy"}]}]

  """
  def to_relations(%{where: wheres}, db) do
    to_relations(wheres, [], db, 0)
  end

  def to_relations([], relations, _db, _relcounter) do
    # Enum.reverse(relations)
    relations
  end

  def to_relations([where | wheres], relations, db, relation_number) do
    symbols = compute_symbols(where)

    tuples =
      filter_tuples(where, db)

    relation = %{symbols: symbols, tuples: tuples, where: where, relation_number: relation_number}
    relations = [relation | relations]

    case find_joins(relations) do
      :no_join ->
        IO.inspect("no join, continue as normal")
        to_relations(wheres, relations, db, relation_number + 1)
      join_info ->
        IO.inspect(join_info, label: "to join")
        new_relations = join(join_info, relations, db, relation_number + 1)
        to_relations(wheres, new_relations, db, relation_number + 1)
    end


    # case find_common_variable_relations(relations) do
    #   [] ->
    #     to_relations(wheres, relations, db)

    #   join_vars ->
    #     new_relations = do_join(join_vars, relations, db)
    #     to_relations(wheres, new_relations, db)
    # end
    # to_relations(wheres, relations, db, relation_number + 1)
  end

  @datom_schema_eavt [:e, :a, :v, :t]

  @doc """
      iex> import Elog.Query
      iex> where = [~q(e), :name, ~q(name)]
      iex> compute_symbols(where)
      %{{:var, :e} => :e, {:var, :name} => :v}
  """
  def compute_symbols(where) do
    where
    |> Enum.zip(@datom_schema_eavt)
    |> Enum.filter(fn {k, v} -> var?(k) end)
    |> Enum.into(%{})
  end

  @doc """
      iex> import Elog.Query
      iex> where = [~q(e), :name, ~q(name)]
      iex> db = [%Elog.Datom{e: 1, a: :name, v: "Bill", t: 18141}, %Elog.Datom{e: 2, a: :name, v: "Sandy", t: 22222}]
      iex> filter_tuples(where, db)
      [%Elog.Datom{e: 1, a: :name, v: "Bill", t: 18141}, %Elog.Datom{e: 2, a: :name, v: "Sandy", t: 22222}]

      iex> import Elog.Query
      iex> where = [~q(e), :name, "Marsha"]
      iex> db = [%Elog.Datom{e: 1, a: :name, v: "Bill", t: 18141}]
      iex> filter_tuples(where, db)
      []

      iex> import Elog.Query
      iex> where = [~q(e), :name, "Marsha"]
      iex> db = [%Elog.Datom{e: 1, a: :name, v: "Marsha", t: 18141}]
      iex> filter_tuples(where, db)
      [%Elog.Datom{e: 1, a: :name, v: "Marsha", t: 18141}]

      iex> import Elog.Query
      iex> where = [~q(e), :age, 23]
      iex> db = [%Elog.Datom{e: 1, a: :name, v: "Marsha", t: 18141}]
      iex> filter_tuples(where, db)
      []
  """
  def filter_tuples([e, a, v], db) do
    db
    |> Enum.filter(fn tuple ->
      tuple.a == a
    end)
    |> Enum.filter(fn tuple ->
      (var_match?(e, :e, tuple) || literal_match?(e, :e, tuple)) &&
        (var_match?(v, a, tuple) || literal_match?(v, a, tuple))
    end)
  end

  def var_match?({:var, _} = _term, _lookup, _tuple), do: true
  def var_match?(_term, _lookup, _tuple), do: false

  def literal_match?(literal, given_attribute, tuple) do
    tuple_attribute = Map.fetch!(tuple, :a)

    if tuple_attribute == given_attribute do
      tuple_value = Map.fetch!(tuple, :v)
      tuple_value == literal
    else
      false
    end
  end

  def find_joins(relations) do
    relations_graphs = relations_graph(relations)
    [rel | rels] = relations
    %{symbols: symbols} = rel

    join_vars =
      Enum.filter(symbols, fn {{:var, var}, field} ->
        DG.in_degree(relations_graphs, var) > 1
      end)

    IO.inspect(join_vars, label: "JV")

    variable_node_sets =
      Enum.reduce(join_vars, %{}, fn {{:var, var}, field}, acc ->
        Map.put(acc, var, DG.predecessors(relations_graphs, var))
      end)

    if Enum.empty?(variable_node_sets) do
      :no_join
    else
      sets = Map.values(variable_node_sets)
      union =
        Enum.reduce(sets, fn val, acc -> MapSet.union(val, acc) end)
      intersection =
        Enum.reduce(sets, fn val, acc -> MapSet.intersection(val, acc) end)
      diff = MapSet.difference(union, intersection)
      %{left: intersection,
        right: diff,
        join_vars: join_vars}
    end
  end

  def relations_graph(relations) do
    Enum.reduce(relations, %{}, fn %{symbols: symbols, relation_number: relation_number} = rel, acc ->
      s =
        symbols
        |> Enum.map(fn {{:var, var}, field} ->
        var
      end)
      |> MapSet.new()

        Map.put(acc, relation_number, s)
    end)
  end

  def join(
    %{left: left, right: right, join_vars: join_vars} = join_info,
    relations,
    db,
    relation_number
  ) do
    IO.inspect(join_info, label: "vnj")
    IO.inspect(relations, label: "relations")
    IO.inspect(db, label: "db")
    IO.inspect(relation_number, label: "rel number")

    xpro_relations =
      relations
      |> Enum.filter(fn %{relation_number: relation_number} ->
        MapSet.member?(right, relation_number)
      end)

    products = cartesian_product(xpro_relations)

    [%{symbols: left_symbols}, %{symbols: right_symbols}] =
      xpro_relations

    xpro = %{xpro_relations: xpro_relations,
             left_symbols: left_symbols,
             right_symbols: right_symbols}

    jvs =
      join_vars
      |> Enum.map(fn {{:var, var} = jv, _} ->
        jv
      end)
      |> MapSet.new()

    left_var =
      left_symbols
      |> Enum.filter(fn {{:var, var} = jv, _} ->
        MapSet.member?(jvs, jv)
      end)
      |> List.first

    right_var =
      right_symbols
      |> Enum.filter(fn {{:var, var} = jv, _} ->
        MapSet.member?(jvs, jv)
      end)
      |> List.first

    compound_join_key =
      fn {left_rel, right_rel} ->
        {{:var, lvar}, left_field} = left_var
        {{:var, rvar}, right_field} = right_var

        %{lvar => Map.fetch!(left_rel, left_field),
          rvar => Map.fetch!(right_rel, right_field)}
      end

    left_relation_number =
      left
      |> Enum.to_list()
      |> List.first()

    left_relation =
      relations
      |> Enum.filter(fn %{relation_number: relation_number} ->
        relation_number == left_relation_number
      end)
      |> List.first()

    left_join_key =
      fn t ->
        syms = left_relation[:symbols]
        Enum.reduce(syms, %{}, fn {{:var, var}, field}, acc ->
          Map.put(acc, var, Map.fetch!(t, field))
        end)
      end

    combined_symbols = Map.merge(left_symbols, right_symbols)
    # IO.inspect(combined_symbols, label: "CS")
    IO.inspect(left_symbols, label: "ls")
    IO.inspect(right_symbols, label: "rs")
    IO.inspect(join_vars, label: "final join vars")

    joined_tuples =
      Elog.Db.hash_join({left_relation[:tuples], left_join_key}, {products, compound_join_key})
      |> Enum.flat_map(fn {{prod_l, prod_r}, r} ->
        [prod_l, prod_r, r]
      end)
      |> MapSet.new()
      |> IO.inspect(label: "a goddamn join")

      # needs to return an actual new relation,
      # including a new symbol set
      # and a new tuple set

    # IO.inspect(xpro, label: "xpro rels")
  end

  def cartesian_product([
        %{symbols: syms1, tuples: rel_tuples1},
        %{symbols: syms2, tuples: rel_tuples2}
      ]) do
    for tuple1 <- rel_tuples1,
        tuple2 <- rel_tuples2 do
        {tuple1, tuple2} |> IO.inspect(label: "cart prod")
    end
  end

  ##########

  def sigil_q(s, []) do
    {:var, String.to_atom(s)}
  end

  defp var?({:var, _var}), do: true
  defp var?(_), do: false
end
