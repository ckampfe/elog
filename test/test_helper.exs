ExUnit.start()

defmodule TestHelper do
  def test_permutations(variants, f)
      when is_list(variants) and is_function(f) do
    permutations = permutations(variants)

    Enum.each(permutations, fn variant ->
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
