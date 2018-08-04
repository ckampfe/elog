defmodule Elog.Db do
  defstruct dataoms: []

  def new() do
    %__MODULE__{}
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
