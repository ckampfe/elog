defmodule UtilTest do
  use ExUnit.Case, async: true
  import TestHelper

  test "it computes all permutations" do
    data = [1, 2, 3]

    assert MapSet.new([
             [1, 2, 3],
             [1, 3, 2],
             [2, 1, 3],
             [2, 3, 1],
             [3, 1, 2],
             [3, 2, 1]
           ]) == MapSet.new(permutations(data))
  end
end
