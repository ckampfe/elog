defmodule ElogDbTest do
  use ExUnit.Case
  doctest Elog.Db
  alias Elog.Db
  import Elog.Syntax

  describe "query" do
    test "find all entity attributes" do
      query = %{find: [~q(attr)], where: [[1, ~q(attr)]]}
      db = Db.new([%{name: "Marsha", age: 7, size: 98.0}])
      result = Db.query(db, query)

      assert result ==
               MapSet.new([%{attr: :age}, %{attr: :name}, %{attr: :size}])
    end

    test "find all attributes for all entities" do
      query = %{find: [~q(e), ~q(attr)], where: [[~q(e), ~q(attr)]]}

      db =
        Db.new([
          %{name: "Marsha", age: 7, size: 98.0},
          %{kind: "gorgon", eye_color: "blue"}
        ])

      result = Db.query(db, query)

      assert result ==
               MapSet.new([
                 %{attr: :age, e: 1},
                 %{attr: :eye_color, e: 2},
                 %{attr: :kind, e: 2},
                 %{attr: :name, e: 1},
                 %{attr: :size, e: 1}
               ])
    end

    test "find all entities for a given attribute" do
      query = %{find: [~q(e)], where: [[~q(e), :size]]}

      db =
        Db.new([
          %{name: "Marsha", age: 7, size: 98.0},
          %{name: "bill", eye_color: "blue"},
          %{name: "Sue", size: 1_002_242}
        ])

      result = Db.query(db, query)
      assert result == MapSet.new([%{e: 1}, %{e: 3}])
    end

    test "find all attributes" do
      # TODO: introduce wildcard or similar sigil value so that the engine does not carry through the entity variable
      # this will save memory when the variable is not used/needed in the find
      query = %{find: [~q(attr)], where: [[~q(e), ~q(attr)]]}

      db =
        Db.new([
          %{name: "Marsha", age: 7, size: 98.0},
          %{kind: "gorgon", eye_color: "blue"},
          %{geography: "arid"}
        ])

      result = Db.query(db, query)

      assert result ==
               MapSet.new([
                 %{attr: :age},
                 %{attr: :eye_color},
                 %{attr: :geography},
                 %{attr: :kind},
                 %{attr: :name},
                 %{attr: :size}
               ])
    end

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

    test "more direct joins" do
      query = %{
        find: [~q(name), ~q(age), ~q(height)],
        where: [
          [~q(e), :name, ~q(name)],
          [~q(e), :age, ~q(age)],
          [~q(e), :height, ~q(height)]
        ]
      }

      db =
        Db.new([
          %{name: "Bill", age: 2, height: 9},
          %{name: "Jim", age: 4, height: 4},
          %{name: "Gail", age: 842, height: 1},
          %{name: "Robin", age: 0, height: 141},
          %{name: "James", age: 1, height: 4802}
        ])

      result = Db.query(db, query)

      assert result ==
               MapSet.new([
                 %{name: "Bill", age: 2, height: 9},
                 %{name: "Jim", age: 4, height: 4},
                 %{name: "Gail", age: 842, height: 1},
                 %{name: "Robin", age: 0, height: 141},
                 %{name: "James", age: 1, height: 4802}
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
