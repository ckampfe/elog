:ets.new(:test_stats, [:public, :named_table])

ExUnit.start()

defmodule TestHelper do
  def test_permutations(variants, f)
      when is_list(variants) and is_function(f) do
    :ets.update_counter(:test_stats, :total_tests, 1, {:total_tests, 0})

    permutations = permutations(variants)

    Enum.each(permutations, fn variant ->
      :ets.update_counter(:test_stats, :total_permutations, 1, {:total_permutations, 0})
      f.(variant)
    end)
  end

  def permutations([]), do: [[]]

  def permutations(list) do
    for elem <- list,
        rest <- permutations(list -- [elem]) do
      [elem | rest]
    end
  end
end
