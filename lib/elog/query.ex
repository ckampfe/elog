defmodule Elog.Query do
  # names:
  # squirrel
  # squeel

  require MapSet
  require Strand.Protocol.Digraph, as: DG
  require Logger
  alias Elog.Db.Index

  @datom_schema_eavt [:e, :a, :v, :t]

  def validate(%{find: find, where: where} = q)
      when is_map(q) and is_list(find) and is_list(where) do
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

  def to_relations(
        %{find: _find, where: []},
        [relation | relations],
        _db,
        _relcounter
      )
      when is_list(relations) do
    relation
  end

  def to_relations(%{find: _find, where: []}, relations, _db, _relcounter) do
    relations
  end

  def to_relations(
        %{find: _find, where: [where | wheres]} = q,
        relations,
        db,
        relation_number
      ) do
    symbols = compute_symbols(where)

    vars =
      symbols
      |> Enum.map(fn {{:var, _var} = v, _} -> v end)
      |> MapSet.new()

    tuples =
      filter_tuples(where, db)
      |> datoms_to_tuples(symbols)

    relation = %{
      vars: vars,
      tuples: tuples,
      where: where,
      relation_number: relation_number + 1
    }

    relations = [relation | relations]

    case find_joins(relations) do
      :no_join ->
        to_relations(
          Map.put(q, :where, wheres),
          relations,
          db,
          relation_number + 1
        )

      join_info ->
        new_relations =
          join(join_info, relations, where, db, relation_number + 1)

        to_relations(
          Map.put(q, :where, wheres),
          new_relations,
          db,
          relation_number + 1
        )
    end
  end

  defp compute_symbols(where) do
    where
    |> Enum.zip(@datom_schema_eavt)
    |> Enum.filter(fn {k, _v} -> var?(k) end)
    |> Enum.into(%{})
  end

  # [e: var, a: var, v: var]
  defp filter_tuples(
         [{:var, _evar} = _e, {:var, _avar} = _a, {:var, _vvar} = _v],
         _db
       ) do
    raise "not implemented"
  end

  # [e: var, a: var, v: literal]
  defp filter_tuples([{:var, _evar} = _e, {:var, _avar} = _a, _v], _db) do
    raise "not implemented"
  end

  # [e: var, a: literal, v: var]
  defp filter_tuples([{:var, _evar} = _e, a, {:var, _vvar} = _v], %{indexes: %{aevt: aevt}} = _db) do
    Index.get(aevt, a)
  end

  # [e: literal, a: var, v: var]
  defp filter_tuples([_e, {:var, _avar} = _a, {:var, _vvar} = _v], _db) do
    raise "not implemented"
  end

  # [e: var, a: literal, v: literal]
  defp filter_tuples([{:var, _evar} = _e, a, v], %{indexes: %{avet: avet}} = _db) do
    Index.get(avet, {a, v})
  end

  # [e: literal, a: var, v: literal]
  defp filter_tuples([_e, {:var, _avar} = _a, _v], _db) do
    raise "not implemented"
  end

  # [e: literal, a: literal, v: var]
  defp filter_tuples([_e, _a, {:var, _vvar} = _v], _db) do
    raise "not implemented"
  end

  # [e: literal, a: literal, v: literal]
  defp filter_tuples([e, a, v], db) do
    db
    |> Enum.filter(fn tuple ->
      tuple.a == a
    end)
    |> Enum.filter(fn tuple ->
      (var_match?(e, :e, tuple) || literal_match?(e, :e, tuple)) &&
        (var_match?(v, a, tuple) || literal_match?(v, a, tuple))
    end)
  end

  defp datoms_to_tuples(filtered_datoms, symbols) do
    Enum.map(filtered_datoms, fn datom ->
      Enum.reduce(symbols, %{}, fn {{:var, var}, field}, acc ->
        Map.put(acc, var, Map.fetch!(datom, field))
      end)
    end)
  end

  defp var_match?({:var, _} = _term, _lookup, _tuple), do: true
  defp var_match?(_term, _lookup, _tuple), do: false

  defp literal_match?(literal, field, tuple) do
    tuple_attribute = Map.fetch!(tuple, :a)

    if tuple_attribute == field do
      tuple_value = Map.fetch!(tuple, :v)
      tuple_value == literal
    else
      false
    end
  end

  defp find_joins(relations) do
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

      intersection =
        Enum.reduce(sets, fn val, acc -> MapSet.intersection(val, acc) end)

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

  defp relations_graph(relations) do
    Enum.reduce(relations, %{}, fn %{
                                     vars: vars,
                                     relation_number: relation_number
                                   },
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

  defp join(
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

    {products, products_cardinality} = cartesian_product(xpro_relations)

    [%{vars: left_vars}, %{vars: right_vars}] = xpro_relations

    jvs = join_vars |> MapSet.new()

    {:var, lvar} = _left_var =
      left_vars
      |> Enum.filter(fn {:var, _var} = jv ->
        MapSet.member?(jvs, jv)
      end)
      |> List.first()

    {:var, rvar} = _right_var =
      right_vars
      |> Enum.filter(fn {:var, _var} = jv ->
        MapSet.member?(jvs, jv)
      end)
      |> List.first()

    right_count = Enum.count(right)

    compound_join_key =
      if right_count >= 1 do
        fn {left_rel, right_rel} ->
          %{
            rvar => Map.fetch!(right_rel, rvar),
            lvar => Map.fetch!(left_rel, lvar)
          }
        end
      else
        fn {left_rel, _right_rel} ->
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

    left_join_key_syms =
      if right_count > 1 do
        left_relation[:vars]
      else
        join_vars
      end

    left_join_key = fn t ->
      Enum.reduce(left_join_key_syms, %{}, fn {:var, var}, acc ->
        Map.put(acc, var, Map.fetch!(t, var))
      end)
    end

    new_tuples =
      Elog.Db.hash_join(
        {left_relation[:tuples], Enum.count(left_relation[:tuples]),
         left_join_key},
        {products, products_cardinality, compound_join_key}
      )
      |> Enum.map(fn
        {{prod_l, prod_r}, r} ->
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
        relation_number == left_relation_number ||
          relation_number == right_relation_number
      end)

    [
      %{
        vars: new_vars,
        tuples: new_tuples,
        where: where,
        relation_number: relation_number
      }
      | filtered
    ]
  end

  defp cartesian_product([
         %{tuples: rel_tuples1},
         %{tuples: rel_tuples2}
       ]) do
    {ptime, products} =
     :timer.tc(fn ->


       # for tuple1 <- rel_tuples1,
       #     tuple2 <- rel_tuples2,
       #     tuple1 != tuple2 do
       #   {tuple1, tuple2}
       # end

       Stream.flat_map(rel_tuples1, fn tuple1 ->
         Stream.map(rel_tuples2, fn
           tuple2 ->
             {tuple1, tuple2}
         end)
       end)

     end)


    Logger.debug("cart prod time: #{ptime / 1000} milliseconds")

    cardinality = Enum.count(rel_tuples1) * Enum.count(rel_tuples2)

    {products, cardinality}
  end

  ##########

  defp var?({:var, _var}), do: true
  defp var?(_), do: false
end
