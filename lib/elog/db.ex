defmodule Elog.Db do
  import Elog.Datom
  alias Elog.{Query, Relation}
  alias BiMultiMap, as: Multimap
  require Logger

  # TODO:
  # [ ] negation (punt on this for now, negation is a significant change in execution strategy)
  # [ ] functions in clauses
  # [x] built-in aggregates in finds
  # [ ] user-defined aggregates in finds
  # [x] or
  # [ ] cardinality: many (multimaps?)
  # [ ] query planning
  # [ ] and (within or)
  # [x] wildcards, like: [~q(e), :name, _]
  # [ ] retractions
  # [ ] transaction log
  # [ ] parameterized queries (investigate `with` clause?)
  # [x] test all variants of `where` clauses

  defstruct active_indexes: MapSet.new(), indexes: %{}, current_entity_id: 1

  @doc """
      iex> alias Elog.Db
      iex> Db.new()
      #Elog.Db<[entities: 0, active_indexes: [:eavt, :aevt, :avet]]>
  """
  def new() do
    new([])
  end

  @doc """
      iex> import Elog.{Db, Syntax}
      iex> data = [%{name: "Bill", eye_color: "blue"}, %{name: "Suzy", eye_color: "brown", shoe_size: 6}]
      iex> new(data)
      #Elog.Db<[entities: 2, active_indexes: [:eavt, :aevt, :avet]]>
  """
  def new(maps, options \\ %{indexes: [:eavt, :aevt, :avet]})

  def new(maps, %{indexes: indexes} = _options) when is_list(maps) do
    transaction_id = :erlang.monotonic_time()
    entity_ids = Stream.iterate(1, &(&1 + 1))

    idx =
      maps
      |> Stream.zip(entity_ids)
      |> Enum.flat_map(&to_datoms(&1, transaction_id))
      |> create_indexes(indexes)

    # probably a better
    # way to do this, but until then,
    # here we are.
    ids =
      idx[:eavt].data
      |> Multimap.keys()

    current_entity_id =
      case ids do
        [] ->
          0

        _ ->
          Enum.max(ids)
      end

    %__MODULE__{
      active_indexes: indexes,
      indexes: idx,
      current_entity_id: current_entity_id
    }
  end

  @doc """
      iex> alias Elog.Db
      iex> import Elog.Syntax
      iex> query = %{where: [[~q(e), :name, ~q(name)], [~q(e2), :name, ~q(friendname)], [~q(e), :friend, ~q(e2)]], find: [~q(name), ~q(friendname)]}
      iex> db = Db.new([%{name: "Bill"}, %{name: "Sandy", friend: 1}, %{name: "Jim should not appear"}, %{name: "Susy", friend: 2}])
      iex> Db.query(db, query)
      #MapSet<[%{friendname: "Bill", name: "Sandy"}, %{friendname: "Sandy", name: "Susy"}]>

  """
  def query(db, q) do
    q
    |> Query.validate()
    # |> Query.reorder_wheres()
    |> Query.to_relations(db)
    |> Query.extract_finds(q[:find])
    |> Query.compute_aggregates(q[:find])
  end

  defprotocol Index do
    def get(this, value)
    def get(this, value, default)
    def insert(this, datom)
    def put(this, datom)
  end

  defmodule EAVT do
    defstruct data: Multimap.new()
  end

  defmodule AEVT do
    defstruct data: Multimap.new()
  end

  defmodule AVET do
    defstruct data: Multimap.new()
  end

  defimpl Index, for: EAVT do
    def get(this, entity_id) do
      get(this, entity_id, [])
    end

    def get(%{data: data}, entity_id, default) do
      Multimap.get(data, entity_id, default)
    end

    def insert(%{data: data} = this, datom) do
      e = datom(datom, :e)

      new_data = Multimap.put(data, e, datom)

      %{this | data: new_data}
    end

    def put(%{data: data} = this, datom) do
      e = datom(datom, :e)
      a = datom(datom, :e)

      filtered =
        data
        |> Multimap.get(e)
        |> Enum.reject(fn
          datom(a: this_a) ->
            a == this_a
        end)

      new_data = Multimap.delete_key(data, e)

      filtered
      |> Enum.reduce(%{this | data: new_data}, fn d, acc ->
        Index.insert(acc, d)
      end)
      |> Index.insert(datom)
    end
  end

  defimpl Index, for: AEVT do
    def get(this, entity_id) do
      get(this, entity_id, [])
    end

    def get(%{data: data}, attribute_name, default) do
      Multimap.get(data, attribute_name, default)
    end

    def insert(%{data: data} = this, datom) do
      a = datom(datom, :a)

      new_data = Multimap.put(data, a, datom)

      %{this | data: new_data}
    end

    def put(%{data: data} = this, datom) do
      a = datom(datom, :a)
      e = datom(datom, :e)

      filtered =
        data
        |> Multimap.get(a)
        |> Enum.reject(fn datom(e: this_e) ->
          e == this_e
        end)

      new_data = Multimap.delete_key(data, a)

      filtered
      |> Enum.reduce(%{this | data: new_data}, fn d, acc ->
        Index.insert(acc, d)
      end)
      |> Index.insert(datom)
    end
  end

  defimpl Index, for: AVET do
    def get(this, av) do
      get(this, av, [])
    end

    def get(%{data: data}, av, default) do
      Multimap.get(data, av, default)
    end

    def insert(%{data: data} = this, datom) do
      av = {datom(datom, :a), datom(datom, :v)}
      new_data = Multimap.put(data, av, datom)
      %{this | data: new_data}
    end

    def put(%{data: data} = this, datom) do
      av = {datom(datom, :a), datom(datom, :v)}

      datom_e = datom(datom, :e)
      datom_t = datom(datom, :t)

      to_remove =
        data
        |> Multimap.values()
        |> Enum.filter(fn datom(e: this_e) ->
          datom_e == this_e
        end)
        |> Enum.map(fn datom(a: this_a, v: this_v) ->
          {this_a, this_v}
        end)
        |> Enum.flat_map(fn av ->
          data
          |> Multimap.get(av)
          |> Enum.reject(fn datom(t: this_t) ->
            datom_t == this_t
          end)
        end)
        |> Enum.map(fn datom(a: this_a, v: this_v) ->
          {this_a, this_v}
        end)

      new_data =
        Enum.reduce(to_remove, Multimap.delete(data, av), fn av, acc ->
          Multimap.delete_key(acc, av)
        end)

      Index.insert(%{this | data: new_data}, datom)
    end
  end

  @doc """
      iex> import Elog.Syntax
      iex> alias Elog.Db
      iex> db = Db.new()
      iex> db = Db.transact(db, [%{name: "Bill"}])
      iex> Db.query(db, %{find: [~q(e), ~q(name)], where: [[~q(e), :name, ~q(name)]]})
      #MapSet<[%{e: 1, name: "Bill"}]>

      iex> import Elog.Syntax
      iex> alias Elog.Db
      iex> db = Db.new()
      iex> db = Db.transact(db, [%{"elog/id": 1, name: "Bill"}])
      iex> db = Db.transact(db, [%{"elog/id": 1, name: "Jamie"}])
      iex> Db.query(db, %{find: [~q(e), ~q(name)], where: [[~q(e), :name, ~q(name)]]})
      #MapSet<[%{e: 1, name: "Jamie"}]>
  """
  def transact(db, [m | _rest_of_maps] = data) when is_map(m) do
    transaction_id = :erlang.monotonic_time()
    current_entity_id = db.current_entity_id
    transact(db, data, transaction_id, current_entity_id)
  end

  defp transact(db, [], _transaction_id, current_entity_id) do
    %{db | current_entity_id: current_entity_id}
  end

  defp transact(db, [m | rest_of_maps], transaction_id, current_entity_id) do
    entity_id = Map.get(m, :"elog/id", current_entity_id + 1)
    datoms = to_datoms({m, entity_id}, transaction_id)

    indexes =
      Enum.reduce(db.indexes, db.indexes, fn {index_kind, index}, idxs ->
        new_index =
          Enum.reduce(datoms, index, fn datom, idx ->
            Index.put(idx, datom)
          end)

        Map.put(idxs, index_kind, new_index)
      end)

    transact(
      %{db | indexes: indexes},
      rest_of_maps,
      transaction_id,
      if entity_id == current_entity_id + 1 do
        entity_id
      else
        current_entity_id
      end
    )
  end

  def initialize_index(:eavt), do: %EAVT{}
  def initialize_index(:aevt), do: %AEVT{}
  def initialize_index(:avet), do: %AVET{}
  def initialize_index(:vaet), do: raise("VAET indexes are not implemented")

  defp create_indexes(datoms, indexes) do
    initial_indexes =
      Enum.reduce(indexes, %{}, fn index, acc ->
        Map.put(acc, index, initialize_index(index))
      end)

    Enum.reduce(initial_indexes, initial_indexes, fn {index_kind, index}, idxs ->
      new_index =
        Enum.reduce(datoms, index, fn datom, idx ->
          Index.insert(idx, datom)
        end)

      Map.put(idxs, index_kind, new_index)
    end)
  end

  defp to_datoms({map, entity_id}, transaction_id) do
    Enum.map(map, fn {k, v} ->
      datom(e: entity_id, a: k, v: v, t: transaction_id)
    end)
    |> MapSet.new()
  end

  def hash_join(
        {left_relation, left_join_function} = left,
        {right_relation, right_join_function} = right,
        relation_number
      )
      when is_function(left_join_function) and is_function(right_join_function) do
    left_cardinality = Enum.count(left_relation.tuples)
    right_cardinality = Enum.count(right_relation.tuples)

    # reorder if cardinality indicates we should
    {{left_relation, left_join_function}, {right_relation, right_join_function}} =
      if left_cardinality < right_cardinality do
        {left, right}
      else
        {right, left}
      end

    left_tuples = left_relation.tuples
    right_tuples = right_relation.tuples

    hashed_smaller =
      Enum.reduce(left_tuples, Multimap.new(), fn row, acc ->
        join_attr_value = left_join_function.(row)
        Multimap.put(acc, join_attr_value, row)
      end)

    new_tuples =
      Enum.reduce(right_tuples, [], fn row, acc ->
        case Multimap.get(hashed_smaller, right_join_function.(row)) do
          [] ->
            acc

          match_rows ->
            Enum.reduce(match_rows, acc, fn v, acc2 ->
              [{row, v} | acc2]
            end)
        end
      end)
      |> Enum.map(fn
        {{c1, c2}, r} ->
          Enum.reduce([c1, c2, r], %{}, fn val, acc ->
            Map.merge(acc, val)
          end)

        {l, {c1, c2}} ->
          Enum.reduce([c1, c2, l], %{}, fn val, acc ->
            Map.merge(acc, val)
          end)

        {l, r} ->
          l_keyset = Map.keys(l) |> MapSet.new()
          r_keyset = Map.keys(r) |> MapSet.new()
          diff = MapSet.difference(r_keyset, l_keyset)
          Map.merge(l, Map.take(r, diff))
      end)

    new_vars =
      new_tuples
      |> Enum.take(1)
      |> Enum.flat_map(&Map.keys/1)
      |> Enum.flat_map(fn var -> [{:var, var}] end)

    %Relation{
      vars: new_vars,
      tuples: new_tuples,
      where: nil,
      number: relation_number
    }
  end
end

defimpl Inspect, for: Elog.Db do
  alias BiMultiMap, as: Multimap

  @spec inspect(
          atom() | %{active_indexes: any(), indexes: nil | keyword() | map()},
          any()
        ) :: <<_::64, _::_*8>>
  def inspect(db, _options) do
    # this is probably really bad for large
    # indexes and dbs but that's ok for now
    # because it's all in memory and debugging is good

    indexes = db.indexes
    eavt = indexes[:eavt]
    eavt_data = eavt.data

    # TODO: Fixme
    entities_count =
      eavt_data
      |> Multimap.keys()
      |> Enum.count()

    active_indexes =
      db.active_indexes
      |> Enum.to_list()
      |> inspect()

    "#Elog.Db<[entities: #{entities_count}, active_indexes: #{active_indexes}]>"
  end
end
