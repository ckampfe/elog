defmodule ElogTest do
  use ExUnit.Case
  doctest Elog

  test "greets the world" do
    assert Elog.hello() == :world
  end
end
