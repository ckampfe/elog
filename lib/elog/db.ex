defmodule Elog.Db do
  alias Elog.Datom

  defstruct index: []

  defprotocol Index do
    def new(this, datoms)
    def get(this, k)
    def insert(this, datoms)
  end

  defmodule MapEAVT do
    defstruct data: %{}
  end

  defimpl Index, for: MapEAVT do
    def new(this, datoms) do
      Enum.reduce(datoms, %{}, fn val, acc ->
        IO.inspect(val, label: "val")

        {_, new_acc} =
          Map.get_and_update(acc, val.e, fn
            nil ->
              {nil, [val]}

            current_value ->
              {nil, [val | current_value]}
          end)

        new_acc
      end)
    end
  end

  @doc """
      iex> import Elog.Db
      iex> data = [%{name: "Bill", eye_color: "blue"}, %{name: "Suzy", eye_color: "brown"}]
      iex> new(data)
      :ok
  """
  def new(maps, options \\ %{index: :eavt}) when is_list(maps) and is_map(options) do
    transaction_id = :erlang.monotonic_time()
    datoms = Enum.flat_map(maps, &to_datom(&1, transaction_id))

    index =
      case options[:index] do
        :eavt ->
          Index.new(%MapEAVT{}, datoms)

        other_index ->
          raise "#{other_index} not implemented yet"
      end

    %__MODULE__{index: index}
  end

  defp to_datom(map, transaction_id) do
    entity_id = :erlang.monotonic_time()

    Enum.map(map, fn {k, v} ->
      %Datom{e: entity_id, a: k, v: v, t: transaction_id}
    end)
  end

  @doc """
  """
  def hash_join({r1_tuples, r1_f} = rel1, {r2_tuples, r2_f} = rel2) do
    {{l_tuples, lf} = larger_relation, {s_tuples, sf} = smaller_relation} =
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
