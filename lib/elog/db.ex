defmodule Elog.Db do
  alias Elog.Datom
  alias Elog.Query
  require Logger

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
      |> Map.keys()

    current_entity_id =
      case ids do
        [] ->
          1

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
    |> Query.to_relations(db)
    |> Query.extract_finds(q[:find])
  end

  defprotocol Index do
    def get(this, value)
    def get(this, value, default)
    def insert(this, datom)
    def put(this, datom)
  end

  defmodule EAVT do
    defstruct data: %{}
  end

  defmodule AEVT do
    defstruct data: %{}
  end

  defmodule AVET do
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

    def put(this, datom) do
      e = datom.e

      {_, new_data} =
        Map.get_and_update(this.data, e, fn
          nil ->
            {nil, [datom]}

          old ->
            filtered_olds =
              Enum.reject(old, fn %Elog.Datom{a: this_a} ->
                datom.a == this_a
              end)

            {nil, [datom | filtered_olds]}
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

    def put(this, datom) do
      a = datom.a

      {_, new_data} =
        Map.get_and_update(this.data, a, fn
          nil ->
            {nil, [datom]}

          old ->
            filtered_olds =
              Enum.reject(old, fn %Elog.Datom{e: this_e} ->
                datom.e == this_e
              end)

            {nil, [datom | filtered_olds]}
        end)

      %{this | data: new_data}
    end
  end

  defimpl Index, for: AVET do
    def get(this, av) do
      get(this, av, [])
    end

    def get(this, av, default) do
      Map.get(this.data, av, default)
    end

    def insert(this, datom) do
      av = {datom.a, datom.v}

      {_, new_data} =
        Map.get_and_update(this.data, av, fn
          nil ->
            {nil, [datom]}

          old ->
            {nil, [datom | old]}
        end)

      %{this | data: new_data}
    end

    def put(this, datom) do
      av = {datom.a, datom.v}

      {_, new_data} =
        Map.get_and_update(this.data, av, fn
          nil ->
            {nil, [datom]}

          old ->
            filtered_olds =
              Enum.reject(old, fn %Elog.Datom{e: this_e} ->
                datom.e == this_e
              end)

            {nil, filtered_olds}
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
    entity_id = Map.get(m, :"elog/id", current_entity_id)
    datoms = to_datoms({m, entity_id}, transaction_id)

    indexes =
      Enum.reduce(datoms, db.indexes, fn datom, outer_acc ->
        Enum.reduce(db.indexes, outer_acc, fn {index_kind, _index}, inner_acc ->
          new_index =
            inner_acc
            |> Map.fetch!(index_kind)
            |> Index.put(datom)

          Map.put(inner_acc, index_kind, new_index)
        end)
      end)

    transact(
      %{db | indexes: indexes},
      rest_of_maps,
      transaction_id,
      current_entity_id + 1
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

  defp to_datoms({map, entity_id}, transaction_id) do
    Enum.map(map, fn {k, v} ->
      %Datom{e: entity_id, a: k, v: v, t: transaction_id}
    end)
  end

  def hash_join(
        {_r1_tuples, r1_cardinality, r1_f} = rel1,
        {_r2_tuples, r2_cardinality, r2_f} = rel2
      )
      when is_function(r1_f) and is_function(r2_f) do
    {{l_tuples, l_tuples_cardinality, lf} = _larger_relation,
     {s_tuples, r_tuples_cardinality, sf} = _smaller_relation} =
      if r1_cardinality >= r2_cardinality do
        {rel1, rel2}
      else
        {rel2, rel1}
      end

    # Logger.debug("larger tuples cardinality: #{l_tuples_cardinality}")
    # Logger.debug("smaller tuples cardinality: #{r_tuples_cardinality}")

    {hs_micros, hashed_smaller} =
      :timer.tc(fn ->
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
      end)

    # Logger.debug(
    #   "time to create hashed_smaller: #{hs_micros / 1000} milliseconds"
    # )

    {join_micros, join} =
      :timer.tc(fn ->
        # parts = Enum.chunk_every(l_tuples, 1000)
        # Enum.map(parts, fn part ->
        #   Task.async(fn ->
        #     Enum.flat_map(part, fn row ->
        #       join_val = lf.(row)
        #       match_rows = Map.get(hashed_smaller, join_val, [])
        #       Enum.map(match_rows, fn mr -> {row, mr} end)
        #     end)
        #   end)
        # end)
        # |> Enum.flat_map(fn t -> Task.await(t) end)

        # Enum.flat_map(l_tuples, fn row ->
        #   join_val = lf.(row)
        #   match_rows = Map.get(hashed_smaller, join_val, [])
        #   Enum.map(match_rows, fn mr -> {row, mr} end)
        # end)

        Enum.reduce(l_tuples, [], fn row, acc ->
          join_val = lf.(row)
          match_rows = Map.get(hashed_smaller, join_val)

          if match_rows do
            Enum.reduce(match_rows, acc, fn v, acc2 ->
              [{row, v} | acc2]
            end)
          else
            acc
          end
        end)

        # {:ok, agent} = Agent.start_link(fn -> 1 end)
        # chunk_size = 1000
        # l_tuples_chunks = Enum.chunk_every(l_tuples, chunk_size)
        # Enum.map(l_tuples_chunks, fn chunk ->
        #   Task.async(fn ->
        #     Enum.reduce(chunk, [], fn row, acc ->
        #       join_val = lf.(row)
        #       match_rows = Map.get(hashed_smaller, join_val)

        #       if match_rows do
        #         join =
        #         for match_row <- match_rows do
        #           {row, match_row}
        #         end

        #         join ++ acc
        #       else
        #         acc
        #       end
        #     end) |> MapSet.new()
        #   end)
        # end)
        # |> Enum.reduce(MapSet.new(), fn chunk, acc ->
        #   r = Task.await(chunk, 10_000)
        #   Agent.update(agent, fn old -> old + 1 end)
        #   IO.puts("chunk done")
        #   IO.puts(Agent.get(agent, fn s -> s end))
        #   MapSet.union(r, acc)
        # end)
      end)

    # Logger.debug("time to do join: #{join_micros / 1000} milliseconds")
    join
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
