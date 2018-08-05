defmodule Elog.Query do
  # names:
  # squirrel
  # squeel

  require MapSet
  require Strand.Protocol.Graph, as: Graph
  require Strand.Protocol.Digraph, as: DG
  require Strand.Impl.Digraph, as: Digraph
  require Logger

  @doc """
      iex> import Elog.Query
      iex> query = %{find: [~q(name), ~q(friend_name)], where: [[~q(e), :name, ~q(name)], [~q(e2), :name, ~q(friend_name)], [~q(e), :friend, ~q(e2)]]}
      iex> db = [%Elog.Datom{e: 1, a: :name, v: "Bill", t: 18141}, %Elog.Datom{e: 2, a: :name, v: "Sandy", t: 22222}, %Elog.Datom{e: 2, a: :friend, v: 1, t: 22222}, %Elog.Datom{e: 3, a: :name, v: "Jim should not appear", t: 33333}]
      iex> query(db, query)
      #MapSet<[%{e: 2, e2: 1, name: "Sandy", name2: "Bill"}]>
  """
  def query(db, q) do
    relations =
      q
      |> validate()
      |> to_relations(db)
      |> extract_finds(q[:find])
  end

  def validate(%{find: find, where: where} = q) when is_map(q) and is_list(find) and is_list(where) do
    q
  end

  def extract_finds(tuples, find) do
    Enum.map(tuples, fn tuple ->
      Enum.reduce(find, %{}, fn {:var, var}, acc ->
        Map.put(acc, var, Map.fetch!(tuple, var))
      end)
    end)
  end

  @doc """
      iex> import Elog.Query
      iex> query = %{where: [[~q(e), :name, ~q(name)], [~q(e2), :name, ~q(name2)], [~q(e), :friend, ~q(e2)]], find: []}
      iex> db = [%Elog.Datom{e: 1, a: :name, v: "Bill", t: 18141}, %Elog.Datom{e: 2, a: :name, v: "Sandy", t: 22222}, %Elog.Datom{e: 2, a: :friend, v: 1, t: 22222}, %Elog.Datom{e: 3, a: :name, v: "Jim should not appear", t: 33333}]
      iex> to_relations(query, db)
      #MapSet<[%{e: 2, e2: 1, name: "Sandy", name2: "Bill"}]>

      iex> import Elog.Query
      iex> query = %{where: [[~q(e), :name, ~q(name)], [~q(e2), :name, ~q(friendname)], [~q(e), :friend, ~q(e2)]], find: []}
      iex> db = [%Elog.Datom{e: 1, a: :name, v: "Bill", t: 18141}, %Elog.Datom{e: 2, a: :name, v: "Sandy", t: 22222}, %Elog.Datom{e: 2, a: :friend, v: 1, t: 22222}, %Elog.Datom{e: 3, a: :name, v: "Jim should not appear", t: 33333}, %Elog.Datom{e: 4, a: :name, v: "Susy", t: 44444}, %Elog.Datom{e: 4, a: :friend, v: 2, t: 44444}]
      iex> to_relations(query, db)
      #MapSet<[%{e: 2, e2: 1, friendname: "Bill", name: "Sandy"}, %{e: 4, e2: 2, friendname: "Sandy", name: "Susy"}]>

      iex> import Elog.Query
      iex> query = %{where: [[~q(e), :name, ~q(name)], [~q(e2), :name, ~q(name)]], find: []}
      iex> db = [%Elog.Datom{e: 1, a: :name, v: "Bill", t: 18141}, %Elog.Datom{e: 1, a: :eye_color, v: :blue, t: 18141}, %Elog.Datom{e: 2, a: :name, v: "Bill", t: 22222}]
      iex> to_relations(query, db)
      #MapSet<[%{e: 1, e2: 1, name: "Bill"}, %{e: 1, e2: 2, name: "Bill"}, %{e: 2, e2: 1, name: "Bill"}, %{e: 2, e2: 2, name: "Bill"}]>

  """
  def to_relations(%{find: find, where: wheres} = q, db) do
    to_relations(q, [], db, 0)
  end

  def to_relations(%{find: find, where: []}, relations, _db, _relcounter) do
    relations
  end

  def to_relations(%{find: find, where: [where | wheres]} = q, relations, db, relation_number) do
    Logger.debug("building relation #{relation_number}")
    symbols = compute_symbols(where)

    tuples =
      filter_tuples(where, db)
      |> datoms_to_tuples(symbols)

    relation =
      %{symbols: symbols,
        tuples: tuples,
        where: where,
        relation_number: relation_number}

    relations = [relation | relations]

    case find_joins(relations) do
      :no_join ->
        Logger.debug("no join, continue as normal")
        to_relations(Map.put(q, :where, wheres), relations, db, relation_number + 1)
      join_info ->
        Logger.debug("join detected")
        new_relations = join(join_info, relations, db, relation_number + 1)
        to_relations(Map.put(q, :where, wheres), new_relations, db, relation_number + 1)
    end
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

  def datoms_to_tuples(filtered_datoms, symbols) do
    Enum.map(filtered_datoms, fn datom ->
      Enum.reduce(symbols, %{}, fn {{:var, var}, field}, acc ->
        Map.put(acc, var, Map.fetch!(datom, field))
      end)
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


      if diff == MapSet.new() do
        %{left: Enum.take(union, 1) |> MapSet.new(),
          right: union |> Enum.drop(1) |> Enum.take(1) |> MapSet.new(),
          join_vars: join_vars}
      else
        %{left: intersection,
          right: diff,
          join_vars: join_vars}
      end
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
    xpro_relations =
      if Enum.count(right) > 1 do
        relations
        |> Enum.filter(fn %{relation_number: relation_number} ->
          MapSet.member?(right, relation_number)
        end)
      else
        relations
      end

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

        if Enum.count(right) >= 1 do
          %{lvar => Map.fetch!(left_rel, lvar),
            rvar => Map.fetch!(right_rel, rvar)}
        else
          %{lvar => Map.fetch!(left_rel, lvar)}
        end
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
        syms =
          if Enum.count(right) > 1 do
            left_relation[:symbols]
          else
            join_vars
          end

        Enum.reduce(syms, %{}, fn {{:var, var}, field}, acc ->
          Map.put(acc, var, Map.fetch!(t, var))
        end)
      end

    combined_symbols = Map.merge(left_symbols, right_symbols)

    joined_tuples =
      Elog.Db.hash_join({left_relation[:tuples], left_join_key}, {products, compound_join_key})
      |> Enum.map(fn {{prod_l, prod_r}, r} ->
        Enum.reduce([prod_l, prod_r, r], %{}, fn rel, acc ->
          Map.merge(rel, acc)
        end)
      end)
      |> MapSet.new()


     joined_tuples

    # TODO:
    # needs to return an actual new relation,
    # including a new symbol set
    # and a new tuple set
  end

  def cartesian_product([
        %{symbols: syms1, tuples: rel_tuples1},
        %{symbols: syms2, tuples: rel_tuples2}
      ]) do
    for tuple1 <- rel_tuples1,
        tuple2 <- rel_tuples2,
        tuple1 != tuple2
      do
        {tuple1, tuple2}
    end
  end

  ##########

  def sigil_q(s, []) do
    {:var, String.to_atom(s)}
  end

  defp var?({:var, _var}), do: true
  defp var?(_), do: false
end
