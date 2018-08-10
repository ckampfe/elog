defmodule Elog.Db do
  alias Elog.Datom
  alias Elog.Query
  require Logger

  defstruct active_indexes: MapSet.new(), indexes: %{}, current_entity_id: 1

  @doc """
      iex> import Elog.{Db, Syntax}
      iex> data = [%{name: "Bill", eye_color: "blue"}, %{name: "Suzy", eye_color: "brown", shoe_size: 6}]
      iex> new(data)
      #Elog.Db<[entities: 2, active_indexes: [:eavt, :aevt]]>
  """
  def new(maps, options \\ %{indexes: [:eavt, :aevt]})

  def new(maps, %{indexes: indexes} = _options) when is_list(maps) do
    transaction_id = :erlang.monotonic_time()
    entity_ids = Stream.iterate(1, &(&1 + 1))

    idx =
      maps
      |> Stream.zip(entity_ids)
      |> Enum.flat_map(&to_datom(&1, transaction_id))
      |> create_indexes(indexes)

    # probably a better
    # way to do this, but until then,
    # here we are.
    current_entity_id =
      idx[:eavt].data
      |> Map.keys()
      |> Enum.max()

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
    |> Query.to_relations(db)
    |> Query.extract_finds(q[:find])
  end

  defprotocol Index do
    def get(this, value)
    def get(this, value, default)
    def insert(this, datom)
  end

  defmodule EAVT do
    defstruct data: %{}
  end

  defmodule AEVT do
    defstruct data: %{}
  end

  defimpl Index, for: EAVT do
    def get(this, entity_id) do
      get(this, entity_id, [])
    end

    def get(this, entity_id, default) do
      Map.get(this.data, entity_id, default)
    end

    def insert(this, datom) do
      e = datom.e

      {_, new_data} =
        Map.get_and_update(this.data, e, fn
          nil ->
            {nil, [datom]}

          old ->
            {nil, [datom | old]}
        end)

      %{this | data: new_data}
    end
  end

  defimpl Index, for: AEVT do
    def get(this, entity_id) do
      get(this, entity_id, [])
    end

    def get(this, attribute_name, default) do
      Map.get(this.data, attribute_name, default)
    end

    def insert(this, datom) do
      a = datom.a

      {_, new_data} =
        Map.get_and_update(this.data, a, fn
          nil ->
            {nil, [datom]}

          old ->
            {nil, [datom | old]}
        end)

      %{this | data: new_data}
    end
  end

  def initialize_index(:eavt), do: %EAVT{}
  def initialize_index(:aevt), do: %AEVT{}
  def initialize_bytes(:avet), do: raise("AVET indexes are not implemented")
  def initialize_bytes(:vaet), do: raise("VAET indexes are not implemented")

  defp create_indexes(datoms, indexes) do
    initial_indexes =
      Enum.reduce(indexes, %{}, fn index, acc ->
        Map.put(acc, index, initialize_index(index))
      end)

    Enum.reduce(datoms, initial_indexes, fn datom, outer_acc ->
      Enum.reduce(initial_indexes, outer_acc, fn {index_kind, _index},
                                                 inner_acc ->
        new_index =
          inner_acc
          |> Map.fetch!(index_kind)
          |> Index.insert(datom)

        Map.put(inner_acc, index_kind, new_index)
      end)
    end)
  end

  defp to_datom({map, entity_id}, transaction_id) do
    Enum.map(map, fn {k, v} ->
      %Datom{e: entity_id, a: k, v: v, t: transaction_id}
    end)
  end

  def hash_join({r1_tuples, r1_f} = rel1, {r2_tuples, r2_f} = rel2)
      when is_function(r1_f) and is_function(r2_f) do
    {{l_tuples, lf} = _larger_relation, {s_tuples, sf} = _smaller_relation} =
      if Enum.count(r1_tuples) >= Enum.count(r2_tuples) do
        {rel1, rel2}
      else
        {rel2, rel1}
      end

    hashed_smaller =
      Enum.reduce(s_tuples, %{}, fn row, acc ->
        join_attr_value = sf.(row)

        {_, rows} =
          Map.get_and_update(acc, join_attr_value, fn
            nil ->
              {nil, [row]}

            existing ->
              {nil, [row | existing]}
          end)

        rows
      end)

    Enum.reduce(l_tuples, [], fn row, acc ->
      join_val = lf.(row)
      match_rows = Map.get(hashed_smaller, join_val)

      if match_rows do
        join =
          for match_row <- match_rows do
            {row, match_row}
          end

        join ++ acc
      else
        acc
      end
    end)
  end
end

defimpl Inspect, for: Elog.Db do
  def inspect(db, _options) do
    # this is probably really bad for large
    # indexes and dbs but that's ok for now
    # because it's all in memory and debugging is good

    indexes = db.indexes
    eavt = indexes[:eavt]
    eavt_data = eavt.data

    entities_count =
      eavt_data
      |> Map.keys()
      |> Enum.count()

    active_indexes =
      db.active_indexes
      |> Enum.to_list()
      |> inspect()

    "#Elog.Db<[entities: #{entities_count}, active_indexes: #{active_indexes}]>"
  end
end
