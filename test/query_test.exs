defmodule ElogQueryTest do
  use ExUnit.Case
  doctest Elog.Query
  import Elog.Query

  # test "simple queries" do
  #   query = %{find: [~q(name), ~q(friend_name)],
  #             where: [[~q(e), :name, ~q(name)],
  #                     [~q(e2), :name, ~q(friend_name)],
  #                     [~q(e), :friend, ~q(e2)]]
  #            }
  #   db = [%Elog.Datom{e: 1, a: :name, v: "Bill", t: 18141},
  #         %Elog.Datom{e: 2, a: :name, v: "Sandy", t: 22222},
  #         %Elog.Datom{e: 2, a: :friend, v: 1, t: 22222},
  #         %Elog.Datom{e: 3, a: :name, v: "Jim should not appear", t: 33333}]

  #   result = query(db, query)
  #   assert result == MapSet.new([%{friend_name: "Bill", name: "Sandy"}])


  # end


  # test "does find *" do
  #   query = %{find: [:*],
  #             where: [[~q(e), :name, ~q(name)],
  #                     [~q(e2), :name, ~q(friend_name)],
  #                     [~q(e), :friend, ~q(e2)]]
  #            }
  #   db = [%Elog.Datom{e: 1, a: :name, v: "Bill", t: 18141},
  #         %Elog.Datom{e: 2, a: :name, v: "Sandy", t: 22222},
  #         %Elog.Datom{e: 2, a: :friend, v: 1, t: 22222},
  #         %Elog.Datom{e: 3, a: :name, v: "Jim should not appear", t: 33333}]

  #   result = query(db, query)
  #   assert result == MapSet.new([%{friend_name: "Bill", name: "Sandy", e: 2, e2: 1}])
  # end
end
