defmodule ElogDbTest do
  use ExUnit.Case
  doctest Elog.Db
  alias Elog.Db
  import Elog.Syntax

  describe "query" do
    # TODO:
    # write tests and implementation for:
    # %{find: [~q(attr)], where: [[~q(e), ~q(attr)]]}
    # to find all attrs in db (probably AEVT to find all attrs
    # as keys, and then flat_map to get all values)
    # and:
    # %{find: [~q(attr)], where: [[~q(e), "literal" _]]}
    # (with wildcard or not? unclear)
    # to find all entities with a literal attribute
    # irrespective of attribute value (AEVT probably)


    test "literal no attribute" do
      query = %{find: [~q(e)], where: [[~q(e), :age, 23]]}
      db = Db.new([%{name: "Marsha"}])
      result = Db.query(db, query)
      assert result == MapSet.new([])
    end

    test "literal no value" do
      query = %{find: [~q(e)], where: [[~q(e), :name, "Marsha"]]}
      db = Db.new([%{name: "Bill"}])
      result = Db.query(db, query)

      assert result == MapSet.new([])
    end

    test "find all vars" do
      query = %{find: [~q(e), ~q(name)], where: [[~q(e), :name, ~q(name)]]}
      db = Db.new([%{name: "Bill"}, %{name: "Sandy"}])
      result = Db.query(db, query)

      assert result ==
               MapSet.new([%{e: 1, name: "Bill"}, %{e: 2, name: "Sandy"}])
    end

    test "double join" do
      query = %{
        find: [~q(e), ~q(e2), ~q(name), ~q(name2)],
        where: [
          [~q(e), :name, ~q(name)],
          [~q(e2), :name, ~q(name2)],
          [~q(e), :friend, ~q(e2)]
        ]
      }

      db =
        Db.new([
          %{name: "Bill"},
          %{name: "Sandy", friend: 1},
          %{name: "Jim should not appear"}
        ])

      result = Db.query(db, query)

      assert result ==
               MapSet.new([%{e: 2, e2: 1, name: "Sandy", name2: "Bill"}])
    end

    test "direct join" do
      query = %{
        find: [~q(e), ~q(e2), ~q(name)],
        where: [[~q(e), :name, ~q(name)], [~q(e2), :name, ~q(name)]]
      }

      db = Db.new([%{name: "Bill", eye_color: "blue"}, %{name: "Bill"}])
      result = Db.query(db, query)

      assert result ==
               MapSet.new([
                 %{e: 1, e2: 1, name: "Bill"},
                 %{e: 1, e2: 2, name: "Bill"},
                 %{e: 2, e2: 1, name: "Bill"},
                 %{e: 2, e2: 2, name: "Bill"}
               ])
    end

    test "literal value match" do
      query = %{find: [~q(e)], where: [[~q(e), :name, "Marsha"]]}
      db = Db.new([%{name: "Marsha"}])
      result = Db.query(db, query)
      assert result == MapSet.new([%{e: 1}])
    end

    test "literal with var" do
      query = %{
        find: [~q(e), ~q(name)],
        where: [[~q(e), :name, "Marsha"], [~q(e), :name, ~q(name)]]
      }

      db = Db.new([%{name: "Marsha"}, %{name: "Bill should not appear"}])
      result = Db.query(db, query)

      assert result == MapSet.new([%{e: 1, name: "Marsha"}])
    end

    test "two literals with vars" do
      query = %{
        find: [~q(e), ~q(name), ~q(eye_color)],
        where: [
          [~q(e), :name, "Marsha"],
          [~q(e), :eye_color, "Blue"],
          [~q(e), :eye_color, ~q(eye_color)],
          [~q(e), :name, ~q(name)]
        ]
      }

      db =
        Db.new([
          %{name: "Marsha", eye_color: "Blue"},
          %{name: "Marsha", eye_color: "red should not be here"},
          %{name: "Bill should not appear"}
        ])

      result = Db.query(db, query)

      assert result == MapSet.new([%{e: 1, eye_color: "Blue", name: "Marsha"}])
    end
  end

  describe "transact" do
    test "it adds bare data" do
      db = Db.new()
      db = Db.transact(db, [%{name: "Bill"}])

      result =
        Db.query(db, %{
          find: [~q(e), ~q(name)],
          where: [[~q(e), :name, ~q(name)]]
        })

      assert db.current_entity_id == 2
      assert result == MapSet.new([%{e: 1, name: "Bill"}])
    end

    test "it does mutations with explicit elog/id" do
      db = Db.new()
      db = Db.transact(db, [%{"elog/id": 1, name: "Bill"}])
      db = Db.transact(db, [%{"elog/id": 1, name: "Jamie"}])

      result =
        Db.query(db, %{
          find: [~q(e), ~q(name)],
          where: [[~q(e), :name, ~q(name)]]
        })

      assert db.current_entity_id == 3
      assert result == MapSet.new([%{e: 1, name: "Jamie"}])
    end

    test "it does mutations with explicit and implicit elog/id" do
      db = Db.new()
      db = Db.transact(db, [%{name: "Bill"}])
      db = Db.transact(db, [%{"elog/id": 1, name: "Jamie"}])

      result =
        Db.query(db, %{
          find: [~q(e), ~q(name)],
          where: [[~q(e), :name, ~q(name)]]
        })

      assert db.current_entity_id == 3
      assert result == MapSet.new([%{e: 1, name: "Jamie"}])
    end
  end
end
