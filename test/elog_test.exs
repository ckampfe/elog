defmodule ElogTest do
  use ExUnit.Case, async: true
  doctest Elog

  test "greets the world" do
    assert Elog.hello() == :world
  end
end
