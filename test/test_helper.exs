ExUnit.start()

defmodule TestHelper do
  def all_permutations(variants, f) when is_list(variants) and is_function(f) do
    permutations = do_permutations(variants)

    Enum.each(permutations, fn variant ->
      f.(variant)
    end)
  end

  # found somewhere on the internet
  def do_permutations([]), do: [[]]

  def do_permutations(list) do
    for elem <- list,
        rest <- do_permutations(list -- [elem]) do
      [elem | rest]
    end
  end
end
