defmodule Elog.Query do
  # names:
  # squirrel
  # squeel

  require MapSet
  require Strand.Protocol.Digraph, as: DG
  require Logger
  alias Elog.Db.Index
  import Elog.Datom
  alias Elog.Relation
  alias BiMultiMap, as: Multimap

  @datom_schema_eavt [:e, :a, :v, :t]
  @datom_to_record_map %{e: 1, a: 2, v: 3, t: 4}
  @aggregates MapSet.new([:avg, :max, :min, :count, :sum])

  def validate(%{find: find, where: where} = q)
      when is_map(q) and is_list(find) and is_list(where) do
    # TODO: to real query validation here, with real error messages
    q
  end

  # @precedence_table %{not: 1}

  # def reorder_wheres(%{where: where} = q) do
  #   new_wheres =
  #     Enum.sort(where, fn
  #       {t1, _}, {t2, _} ->
  #         Map.get(@precedence_table, t1, 0) >= Map.get(@precedence_table, t2, 0)

  #       {t1, _}, _w2 ->
  #         Map.get(@precedence_table, t1, 0) >= 0

  #       _w1, {t2, _} ->
  #         0 >= Map.get(@precedence_table, t2, 0)

  #       [{:var, _}, _, {:var, _}], [{:var, _}, _, {:var, _}] ->
  #         true

  #       [{:var, _}, _, {:var, _}], [{:var, _}, _, _] ->
  #         false

  #       [{:var, _}, _, _], [{:var, _}, _, {:var, _}] ->
  #         true

  #       [{:var, _}, _, _], [{:var, _}, _, _] ->
  #         true

  #       [{:var, _}, _, _], [_, _, _] ->
  #         false

  #       [_, _, _], [{:var, _}, _, _] ->
  #         true

  #       [{:var, _}, _, {:var, _}], _ ->
  #         false

  #       _, [{:var, _}, _, {:var, _}] ->
  #         true

  #       _, _ ->
  #         false
  #     end)

  #   Map.put(q, :where, new_wheres)
  # end

  def extract_finds(%{vars: vars} = rel, [:*]) do
    extract_finds(rel, vars)
  end

  def extract_finds(%{tuples: tuples}, find) do
    Enum.map(tuples, fn tuple ->
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
              gdata_fields = Enum.map(gdata, fn t -> Map.fetch!(t, agg_field) end)

              agg_return = apply(__MODULE__, agg_op, [gdata_fields])

              Map.put(acc, rename, agg_return)

            {agg_op, {:var, agg_field}}, acc ->
              gdata_fields = Enum.map(gdata, fn t -> Map.fetch!(t, agg_field) end)

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
        %{find: _find, where: [{:or, [or1 | or_rest] = ors} = where | wheres]} = q,
        relations,
        db,
        relation_number
      ) do
    or1_symbols = compute_symbols(or1)
    or_rest_symbols = Enum.map(or_rest, &compute_symbols/1)

    or_expression_errors =
      Enum.reduce(or_rest_symbols, [], fn syms, error_acc ->
        if syms == or1_symbols do
          error_acc
        else
          [syms | error_acc]
        end
      end)

    unless Enum.empty?(or_expression_errors) do
      raise "All :or expression variables must be the same. Non-matching clauses are: #{
              inspect([or1_symbols | or_expression_errors])
            }"
    end

    symbols = or1_symbols

    vars =
      symbols
      |> Enum.map(fn {{:var, _var} = v, _} -> v end)
      |> MapSet.new()

    tuples =
      Enum.flat_map(ors, fn this_or ->
        this_or
        |> filter_tuples(db)
        |> datoms_to_tuples(symbols)
      end)

    relation = %Relation{
      vars: vars,
      tuples: tuples,
      where: ors,
      number: relation_number + 1
    }

    relations = [relation | relations]

    dispatch_to_joins(relations, where, wheres, q, db, relation_number)
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

    relation = %Relation{
      vars: vars,
      tuples: tuples,
      where: where,
      number: relation_number + 1
    }

    relations = [relation | relations]

    dispatch_to_joins(relations, where, wheres, q, db, relation_number)
  end

  defp dispatch_to_joins(relations, where, wheres, q, db, relation_number) do
    case find_joins(relations, where) do
      :no_join ->
        to_relations(
          Map.put(q, :where, wheres),
          relations,
          db,
          relation_number + 1
        )

      %{join_type: :normal} = join_info ->
        new_relations = join(join_info, relations, where, db, relation_number + 1)

        to_relations(
          Map.put(q, :where, wheres),
          new_relations,
          db,
          relation_number + 1
        )
    end
  end

  defp compute_symbols({:or, ors}) do
    Enum.flat_map(ors, &compute_symbols/1)
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
    |> Multimap.values()
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

  defp filter_tuples(
         [{:var, _evar} = _e, a, :_],
         %{indexes: %{aevt: aevt}} = _db
       ) do
    Index.get(aevt, a)
  end

  # [e: wildcard, a: literal, v: var]
  defp filter_tuples([:_, a, {:var, _vvar}], %{indexes: %{aevt: aevt}} = _db) do
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

  defp find_joins([%Relation{vars: vars} = _rel | _rels] = relations, _where) do
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

      intersection = Enum.reduce(sets, fn val, acc -> MapSet.intersection(val, acc) end)

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
      fn %{vars: vars, number: relation_number}, acc ->
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

  defp direct_join(
         %{left: left, right: right, join_vars: join_vars} = join_info,
         relations,
         where,
         _db,
         relation_number
       ) do
    xpro_relations = relations

    join_vars = join_vars |> MapSet.new()

    left_relation_number =
      left
      |> Enum.to_list()
      |> List.first()

    right_relation_number =
      right
      |> Enum.to_list()
      |> List.first()

    left_relation =
      Enum.find(relations, fn %Relation{number: relation_number} ->
        relation_number == left_relation_number
      end)

    right_relation =
      Enum.find(relations, fn %Relation{number: relation_number} ->
        relation_number == right_relation_number
      end)

    {:var, lvar} =
      left_relation.vars
      |> Enum.filter(fn {:var, _var} = join_var ->
        MapSet.member?(join_vars, join_var)
      end)
      |> List.first()

    join_key = fn l ->
      Map.take(l, [lvar])
    end

    left_join_key_syms = join_vars

    left_join_key = fn t ->
      Enum.reduce(left_join_key_syms, %{}, fn {:var, var}, acc ->
        Map.put(acc, var, Map.fetch!(t, var))
      end)
    end

    right_cardinality = Enum.count(right_relation.tuples)

    #########################

    new_tuples =
      case join_info[:join_type] do
        :normal ->
          left_cardinality = Enum.count(left_relation.tuples)

          if left_cardinality < right_cardinality do
            Elog.Db.hash_join(
              {left_relation.tuples, left_join_key},
              {right_relation.tuples, join_key}
            )
          else
            Elog.Db.hash_join(
              {right_relation.tuples, join_key},
              {left_relation.tuples, left_join_key}
            )
          end
          |> MapSet.new()
      end

    new_vars =
      new_tuples
      |> Enum.take(1)
      |> Enum.flat_map(&Map.keys/1)
      |> Enum.flat_map(fn var -> [{:var, var}] end)

    filtered =
      Enum.reject(relations, fn %Relation{number: relation_number} ->
        relation_number == left_relation_number || relation_number == right_relation_number
      end)

    new_relation = %Relation{
      vars: new_vars,
      tuples: new_tuples,
      where: where,
      number: relation_number
    }

    [
      new_relation | filtered
    ]
  end

  defp indirect_join(
         %{left: left, right: right, join_vars: join_vars} = join_info,
         relations,
         where,
         _db,
         relation_number
       ) do
    [left_relation, right_relation | _] =
      xpro_relations =
      relations
      |> Enum.filter(fn %Relation{number: relation_number} ->
        MapSet.member?(right, relation_number)
      end)

    # [left_relation, right_relation | _] = xpro_relations
    join_vars = join_vars |> MapSet.new()

    {:var, lvar} =
      left_relation.vars
      |> Enum.filter(fn {:var, _var} = join_var ->
        MapSet.member?(join_vars, join_var)
      end)
      |> List.first()

    {:var, rvar} =
      right_relation.vars
      |> Enum.filter(fn {:var, _var} = join_var ->
        MapSet.member?(join_vars, join_var)
      end)
      |> List.first()

    compound_join_key = fn {l, r} ->
      %{^lvar => lvar_value} = l
      %{^rvar => rvar_value} = r

      %{
        rvar => rvar_value,
        lvar => lvar_value
      }
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
      Enum.find(relations, fn %Relation{number: relation_number} ->
        relation_number == left_relation_number
      end)

    left_join_key_syms = left_relation.vars

    left_join_key = fn t ->
      Enum.reduce(left_join_key_syms, %{}, fn {:var, var}, acc ->
        Map.put(acc, var, Map.fetch!(t, var))
      end)
    end

    [lr, rr | _rest] = xpro_relations

    {products, products_cardinality} = cartesian_product(lr, rr)

    #########################

    new_tuples =
      case join_info[:join_type] do
        :normal ->
          left_cardinality = Enum.count(left_relation.tuples)

          if left_cardinality < products_cardinality do
            Elog.Db.hash_join(
              {left_relation.tuples, left_join_key},
              {products, compound_join_key}
            )
          else
            Elog.Db.hash_join(
              {products, compound_join_key},
              {left_relation.tuples, left_join_key}
            )
          end
          |> MapSet.new()
      end

    new_vars =
      new_tuples
      |> Enum.take(1)
      |> Enum.flat_map(&Map.keys/1)
      |> Enum.flat_map(fn var -> [{:var, var}] end)

    filtered =
      Enum.reject(relations, fn %Relation{number: relation_number} ->
        relation_number == left_relation_number || relation_number == right_relation_number
      end)

    new_relation = %Relation{
      vars: new_vars,
      tuples: new_tuples,
      where: where,
      number: relation_number
    }

    [
      new_relation | filtered
    ]
  end

  # "and now for the tricky bit"
  defp join(
         %{left: _left, right: right, join_vars: _join_vars} = join_info,
         relations,
         where,
         db,
         relation_number
       ) do
    right_count = Enum.count(right)

    if right_count > 1 do
      indirect_join(join_info, relations, where, db, relation_number)
    else
      direct_join(join_info, relations, where, db, relation_number)
    end
  end

  defp cartesian_product(
         %Relation{tuples: left_tuples},
         %Relation{tuples: right_tuples}
       ) do
    products =
      for tuple1 <- left_tuples,
          tuple2 <- right_tuples do
        {tuple1, tuple2}
      end
      |> MapSet.new()

    cardinality = Enum.count(products)

    {products, cardinality}
  end

  ##########

  defp var?({:var, _var}), do: true
  defp var?(_), do: false
end
