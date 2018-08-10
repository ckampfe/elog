defmodule Elog.Query do
  # names:
  # squirrel
  # squeel

  require MapSet
  # require Strand.Protocol.Graph, as: Graph
  require Strand.Protocol.Digraph, as: DG
  # require Strand.Impl.Digraph, as: Digraph
  require Logger
  alias Elog.Db.Index


  def validate(%{find: find, where: where} = q) when is_map(q) and is_list(find) and is_list(where) do
    q
  end

  def extract_finds(%{tuples: _tuples, vars: vars} = rel, [:*]) do
    extract_finds(rel, vars)
  end
  def extract_finds(%{tuples: tuples}, find) do
    tuples
    |> Enum.map(fn tuple ->
      Enum.reduce(find, %{}, fn {:var, var}, acc ->
        Map.put(acc, var, Map.fetch!(tuple, var))
      end)
    end)
    |> MapSet.new()
  end

  def to_relations(%{find: _find, where: _wheres} = q, db) do
    to_relations(q, [], db, 0)
  end

  def to_relations(%{find: _find, where: []}, [relation | relations], _db, _relcounter) when is_list(relations) do
    relation
  end

  def to_relations(%{find: _find, where: []}, relations, _db, _relcounter) do
    relations
  end

  def to_relations(%{find: _find, where: [where | wheres]} = q, relations, db, relation_number) do
    symbols = compute_symbols(where)
    vars =
      symbols
      |> Enum.map(fn {{:var, _var} = v, _} -> v end)
      |> MapSet.new()

    tuples =
      filter_tuples(where, db)
      |> datoms_to_tuples(symbols)

    relation = %{vars: vars, tuples: tuples, where: where, relation_number: relation_number + 1}

    relations = [relation | relations]

    case find_joins(relations) do
      :no_join ->
        to_relations(Map.put(q, :where, wheres), relations, db, relation_number + 1)

      join_info ->
        new_relations = join(join_info, relations, where, db, relation_number + 1)
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
    |> Enum.filter(fn {k, _v} -> var?(k) end)
    |> Enum.into(%{})
  end

  # [e: var, a: var, v: var]
  def filter_tuples([{:var, _evar} = _e, {:var, _avar} = _a, {:var, _vvar} = _v], _db) do
    raise "not implemented"
  end
  # [e: var, a: var, v: literal]
  def filter_tuples([{:var, _evar} = _e, {:var, _avar} = _a, _v], _db) do
    raise "not implemented"
  end
  # [e: var, a: literal, v: var]
  def filter_tuples([{:var, _evar} = _e, a, {:var, _vvar} = _v], db) do
    indexes = db.indexes
    aevt = indexes[:aevt]
    Index.get(aevt, a)
  end
  # [e: literal, a: var, v: var]
  def filter_tuples([_e, {:var, _avar} = _a, {:var, _vvar} = _v], _db) do
    raise "not implemented"
  end
  # [e: var, a: literal, v: literal]
  def filter_tuples([{:var, _evar} = _e, a, v], db) do
    aevt = db.indexes[:aevt]
    aevt
    |> Index.get(a)
    |> Enum.filter(fn tuple ->
      tuple.v == v
    end)
  end
  # [e: literal, a: var, v: literal]
  def filter_tuples([_e, {:var, _avar} = _a, _v], _db) do
    raise "not implemented"
  end
  # [e: literal, a: literal, v: var]
  def filter_tuples([_e, _a, {:var, _vvar} = _v], _db) do
    raise "not implemented"
  end
  # [e: literal, a: literal, v: literal]
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

  def literal_match?(literal, field, tuple) do
    tuple_attribute = Map.fetch!(tuple, :a)

    if tuple_attribute == field do
      tuple_value = Map.fetch!(tuple, :v)
      tuple_value == literal
    else
      false
    end
  end

  def find_joins(relations) do
    relations_graphs = relations_graph(relations)
    [rel | _rels] = relations
    %{vars: vars} = rel

    join_vars =
      Enum.filter(vars, fn {:var, var} ->
        DG.in_degree(relations_graphs, var) > 1
      end)

    variable_node_sets =
      Enum.reduce(join_vars, %{}, fn {:var, var}, acc ->
        Map.put(acc, var, DG.predecessors(relations_graphs, var))
      end)

    if Enum.empty?(variable_node_sets) do
      :no_join
    else
      sets = Map.values(variable_node_sets)
      union = Enum.reduce(sets, fn val, acc -> MapSet.union(val, acc) end)

      intersection = Enum.reduce(sets, fn val, acc -> MapSet.intersection(val, acc) end)

      diff = MapSet.difference(union, intersection)

      if diff == MapSet.new() do
        %{
          left: Enum.take(union, 1) |> MapSet.new(),
          right: union |> Enum.drop(1) |> Enum.take(1) |> MapSet.new(),
          join_vars: join_vars
        }
      else
        %{left: intersection, right: diff, join_vars: join_vars}
      end
    end
  end

  def relations_graph(relations) do
    Enum.reduce(relations, %{}, fn %{vars: vars, relation_number: relation_number},
                                   acc ->
      s =
        vars
        |> Enum.map(fn {:var, var} ->
          var
        end)
        |> MapSet.new()

      Map.put(acc, relation_number, s)
    end)
  end

  def join(
        %{left: left, right: right, join_vars: join_vars} = _join_info,
        relations,
        where,
        _db,
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

    [%{vars: left_vars}, %{vars: right_vars}] = xpro_relations

    jvs =
      join_vars |> MapSet.new()

    left_var =
      left_vars
      |> Enum.filter(fn {:var, _var} = jv ->
        MapSet.member?(jvs, jv)
      end)
      |> List.first()

    right_var =
      right_vars
      |> Enum.filter(fn {:var, _var} = jv ->
        MapSet.member?(jvs, jv)
      end)
      |> List.first()

    compound_join_key = fn {left_rel, right_rel} ->
      {:var, lvar} = left_var
      {:var, rvar} = right_var

      if Enum.count(right) >= 1 do
        %{
          rvar => Map.fetch!(right_rel, rvar),
          lvar => Map.fetch!(left_rel, lvar)
        }
      else
        %{lvar => Map.fetch!(left_rel, lvar)}
      end
    end

    left_relation_number =
      left
      |> Enum.to_list()
      |> List.first()

    right_relation_number =
      right
      |> Enum.to_list()
      |> List.first()

    left_relation =
      relations
      |> Enum.filter(fn %{relation_number: relation_number} ->
        relation_number == left_relation_number
      end)
      |> List.first()

    left_join_key = fn t ->
      syms =
        if Enum.count(right) > 1 do
          left_relation[:vars]
        else
          join_vars
        end

      Enum.reduce(syms, %{}, fn {:var, var}, acc ->
        Map.put(acc, var, Map.fetch!(t, var))
      end)
    end

    # used?
    # combined_vars = MapSet.union(left_vars, right_vars)

    new_tuples =
      Elog.Db.hash_join({left_relation[:tuples], left_join_key}, {products, compound_join_key})
      |> Enum.map(fn
        {{prod_l, prod_r}, r} = y ->
          Enum.reduce([prod_l, prod_r, r], %{}, fn rel, acc ->
            Map.merge(rel, acc)
          end)
        {r, {prod_l, prod_r}} ->
          Enum.reduce([prod_l, prod_r, r], %{}, fn rel, acc ->
            Map.merge(rel, acc)
          end)
      end)
      |> MapSet.new()

    new_vars =
      new_tuples
      |> Enum.take(1)
      |> Enum.flat_map(&Map.keys/1)
      |> Enum.flat_map(fn var -> [{:var, var}] end)



    filtered =
      Enum.reject(relations, fn %{relation_number: relation_number} ->
        relation_number == left_relation_number || relation_number == right_relation_number
      end)

    [%{vars: new_vars,
       tuples: new_tuples,
       where: where,
       relation_number: relation_number} | filtered]

    # TODO:
    # needs to return an actual new relation,
    # including a new var set
    # and a new tuple set
  end

  def cartesian_product([
        %{tuples: rel_tuples1},
        %{tuples: rel_tuples2}
      ]) do
    for tuple1 <- rel_tuples1,
        tuple2 <- rel_tuples2,
        tuple1 != tuple2 do
      {tuple1, tuple2}
    end
  end

  ##########

  def sigil_q(s, []) do
    {:var, String.to_atom(s)}
  end

  def var?({:var, _var}), do: true
  def var?(_), do: false
end
