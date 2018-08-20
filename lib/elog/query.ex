defmodule Elog.Query do
  # names:
  # squirrel
  # squeel

  require MapSet
  require Strand.Protocol.Digraph, as: DG
  require Logger
  alias Elog.Db.Index
  import Elog.Datom

  @datom_schema_eavt [:e, :a, :v, :t]
  @datom_to_record_map %{e: 1, a: 2, v: 3, t: 4}
  @aggregates MapSet.new([:avg, :max, :min, :count, :sum])

  def validate(%{find: find, where: where} = q)
      when is_map(q) and is_list(find) and is_list(where) do
    # TODO: to real query validation here, with real error messages
    q
  end

  def extract_finds(%{tuples: _tuples, vars: vars} = rel, [:*]) do
    extract_finds(rel, vars)
  end

  def extract_finds(%{tuples: tuples}, find) do
    tuples
    |> Enum.map(fn tuple ->
      Enum.reduce(find, %{}, fn
        {_, {:var, var}, _rename}, acc ->
          %{^var => val} = tuple
          Map.put(acc, var, val)

        {_, {:var, var}}, acc ->
          %{^var => val} = tuple
          Map.put(acc, var, val)

        {:var, var}, acc ->
          %{^var => val} = tuple
          Map.put(acc, var, val)
      end)
    end)
    |> MapSet.new()
  end

  def compute_aggregates(set, find) do
    case find_aggregates(find) do
      {[] = _aggregates, _na} ->
        set

      {aggregates, non_aggregates} ->
        grouped =
          Enum.group_by(set, fn tuple ->
            Map.take(tuple, Enum.map(non_aggregates, fn {:var, v} -> v end))
          end)

        Enum.map(grouped, fn {gkey, gdata} ->
          Enum.reduce(aggregates, %{}, fn
            {agg_op, {:var, agg_field}, rename}, acc ->
              gdata_fields =
                Enum.map(gdata, fn t -> Map.fetch!(t, agg_field) end)

              agg_return = apply(__MODULE__, agg_op, [gdata_fields])

              Map.put(acc, rename, agg_return)

            {agg_op, {:var, agg_field}}, acc ->
              gdata_fields =
                Enum.map(gdata, fn t -> Map.fetch!(t, agg_field) end)

              agg_return = apply(__MODULE__, agg_op, [gdata_fields])

              Map.put(acc, agg_op, agg_return)
          end)
          |> Map.merge(gkey)
        end)
        |> MapSet.new()
    end
  end

  defp find_aggregates(find) do
    results = Enum.group_by(find, &aggregate?/1)
    {Map.get(results, true, []), Map.get(results, false, [])}
  end

  def avg(set) do
    Enum.sum(set) / Enum.count(set)
  end

  def count(set) do
    Enum.count(set)
  end

  def max(set) do
    Enum.max(set)
  end

  def min(set) do
    Enum.min(set)
  end

  def sum(set) do
    Enum.sum(set)
  end

  def aggregate?({agg_term, _, _}) do
    MapSet.member?(@aggregates, agg_term)
  end

  def aggregate?({agg_term, _}) do
    MapSet.member?(@aggregates, agg_term)
  end

  def aggregate?(_), do: false

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
      where
      |> filter_tuples(db)
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

      %{join_type: :normal} = join_info ->
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

  # [e: var, a: var]
  # "all entities and attributes"
  # this is very likely slow, but I'm guessing that AEVT
  # is the fastest, given there are probably fewer attributes
  # than entities, leading to fewer total values,
  # and then fewer values to flatmap over
  defp filter_tuples(
         [{:var, _evar} = _e, {:var, _avar} = _a],
         %{indexes: %{aevt: aevt}} = _db
       ) do
    aevt.data
    |> Map.values()
    |> Enum.flat_map(fn datoms -> datoms end)
  end

  # [e: literal, a: var]
  # "all attributes for a given entity"
  defp filter_tuples([e, {:var, _avar} = _a], %{indexes: %{eavt: eavt}} = _db) do
    Index.get(eavt, e)
  end

  # [e: var, a: literal]
  # "all entities for a given attribute"
  defp filter_tuples([{:var, _evar} = _e, a], %{indexes: %{aevt: aevt}} = _db) do
    Index.get(aevt, a)
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
  defp filter_tuples(
         [{:var, _evar} = _e, a, {:var, _vvar} = _v],
         %{indexes: %{aevt: aevt}} = _db
       ) do
    Index.get(aevt, a)
  end

  # [e: literal, a: var, v: var]
  defp filter_tuples([_e, {:var, _avar} = _a, {:var, _vvar} = _v], _db) do
    raise "not implemented"
  end

  # [e: var, a: literal, v: literal]
  defp filter_tuples(
         [{:var, _evar} = _e, a, v],
         %{indexes: %{avet: avet}} = _db
       ) do
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
  defp filter_tuples([e, a, v], %{indexes: %{avet: avet}} = _db) do
    avet
    |> Index.get({a, v})
    |> Enum.filter(fn datom(e: de) ->
      de == e
    end)

    # db
    # |> Enum.filter(fn tuple ->
    #   tuple.a == a
    # end)
    # |> Enum.filter(fn tuple ->
    #   (var_match?(e, :e, tuple) || literal_match?(e, :e, tuple)) &&
    #     (var_match?(v, a, tuple) || literal_match?(v, a, tuple))
    # end)
  end

  defp datoms_to_tuples(filtered_datoms, symbols) do
    filtered_datoms
    |> Enum.map(fn datom ->
      Enum.reduce(symbols, %{}, fn {{:var, var}, field}, acc ->
        %{^field => datom_index} = @datom_to_record_map
        Map.put(acc, var, elem(datom, datom_index))
      end)
    end)
    |> MapSet.new()
  end

  # defp var_match?({:var, _} = _term, _lookup, _tuple), do: true
  # defp var_match?(_term, _lookup, _tuple), do: false

  # defp literal_match?(literal, field, tuple) do
  #   %{a: tuple_attribute} = tuple

  #   if tuple_attribute == field do
  #     %{v: tuple_value} = tuple
  #     tuple_value == literal
  #   else
  #     false
  #   end
  # end

  defp find_joins([%{vars: vars} = _rel | _rels] = relations) do
    relations_graphs = relations_graph(relations)

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
          join_type: :normal,
          left: Enum.take(union, 1) |> MapSet.new(),
          right: union |> Enum.drop(1) |> Enum.take(1) |> MapSet.new(),
          join_vars: join_vars
        }
      else
        %{
          join_type: :normal,
          left: intersection,
          right: diff,
          join_vars: join_vars
        }
      end
    end
  end

  defp relations_graph(relations) do
    Enum.reduce(
      relations,
      %{},
      fn %{vars: vars, relation_number: relation_number}, acc ->
        s =
          vars
          |> Enum.map(fn {:var, var} ->
            var
          end)
          |> MapSet.new()

        Map.put(acc, relation_number, s)
      end
    )
  end

  # "and now for the tricky bit"
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

    [%{vars: left_vars}, %{vars: right_vars}] = xpro_relations

    jvs = join_vars |> MapSet.new()

    {:var, lvar} =
      left_vars
      |> Enum.filter(fn {:var, _var} = jv ->
        MapSet.member?(jvs, jv)
      end)
      |> List.first()

    {:var, rvar} =
      right_vars
      |> Enum.filter(fn {:var, _var} = jv ->
        MapSet.member?(jvs, jv)
      end)
      |> List.first()

    right_count = Enum.count(right)

    compound_join_key =
      if right_count >= 1 do
        fn xpro_rel ->
          %{^rvar => rvar_value, ^lvar => lvar_value} = xpro_rel

          %{
            rvar => rvar_value,
            lvar => lvar_value
          }
        end
      else
        fn xpro_rel ->
          %{^lvar => lvar_value} = xpro_rel

          %{lvar => lvar_value}
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

    #########################

    [%{tuples: lt}, %{tuples: rt}] = xpro_relations

    r3_r1_valset =
      Task.async(fn ->
        left_relation[:tuples]
        |> Enum.map(fn tuple ->
          Map.fetch!(tuple, lvar)
        end)
        |> MapSet.new()
      end)

    r3_r2_valset =
      Task.async(fn ->
        if right_count >= 1 do
          left_relation[:tuples]
          |> Enum.map(fn tuple ->
            Map.fetch!(tuple, rvar)
          end)
          |> MapSet.new()
        end
      end)

    r3_r1_valset = Task.await(r3_r1_valset)

    r1_filtered_set =
      Task.async(fn ->
        lt
        |> Enum.filter(fn %{^lvar => val} ->
          MapSet.member?(r3_r1_valset, val)
        end)
      end)

    r3_r2_valset = Task.await(r3_r2_valset)

    r2_filtered_set =
      Task.async(fn ->
        rt
        |> Enum.filter(fn %{^rvar => val} ->
          MapSet.member?(r3_r2_valset, val)
        end)
      end)

    r1_filtered_set = Task.await(r1_filtered_set)

    r2_filtered_set = Task.await(r2_filtered_set)

    {products, products_cardinality} =
      cartesian_product([%{tuples: r1_filtered_set}, %{tuples: r2_filtered_set}])

    #########################

    new_tuples =
      Elog.Db.hash_join(
        {left_relation[:tuples], Enum.count(left_relation[:tuples]),
         left_join_key},
        {products, products_cardinality, compound_join_key}
      )
      |> Enum.map(fn
        {l, r} ->
          Map.merge(l, r)
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
    products =
      for tuple1 <- rel_tuples1,
          tuple2 <- rel_tuples2 do
        Map.merge(tuple2, tuple1)
      end
      |> MapSet.new()

    cardinality = Enum.count(products)

    {products, cardinality}
  end

  ##########

  defp var?({:var, _var}), do: true
  defp var?(_), do: false
end
