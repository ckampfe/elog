defmodule ElogDbTest do
  use ExUnit.Case, async: true
  doctest Elog.Db
  alias Elog.Db
  import Elog.Syntax
  import TestHelper

  setup_all do
    on_exit(fn ->
      IO.puts("")
      IO.puts("")
      [total_tests: total_tests] = :ets.lookup(:test_stats, :total_tests)

      [total_permutations: total_permutations] = :ets.lookup(:test_stats, :total_permutations)

      IO.puts(
        "Ran #{total_permutations} total permutations for #{total_tests} tests that had permutations enabled"
      )

      :ok
    end)
  end

  describe "query" do
    test "find all entity attributes" do
      query = %{find: [~q(attr)], where: [[1, ~q(attr)]]}
      db = Db.new([%{name: "Marsha", age: 7, size: 98.0}])
      result = Db.query(db, query)

      assert result == MapSet.new([%{attr: :age}, %{attr: :name}, %{attr: :size}])
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

      assert result == MapSet.new([%{e: 1, name: "Bill"}, %{e: 2, name: "Sandy"}])
    end

    # test "all literals with join" do
    #   query = %{
    #     find: [~q(e), ~q(name)],
    #     where: [[1, :name, "Bill"], [~q(e), :name, ~q(name)]]
    #   }

    #   db = Db.new([%{name: "Bill", age: 81}, %{name: "Red", age: 84}])
    #   result = Db.query(db, query)

    #   assert result == MapSet.new([])
    # end

    test "double join" do
      test_permutations(
        [
          [~q(e), :name, ~q(name)],
          [~q(e2), :name, ~q(name2)],
          [~q(e), :friend, ~q(e2)]
        ],
        fn variant ->
          query = %{
            find: [~q(e), ~q(e2), ~q(name), ~q(name2)],
            where: variant
            # ]
          }

          db =
            Db.new([
              %{name: "Bill"},
              %{name: "Sandy", friend: 1},
              %{name: "Jim should not appear"}
            ])

          result = Db.query(db, query)

          assert result == MapSet.new([%{e: 2, e2: 1, name: "Sandy", name2: "Bill"}])
        end
      )
    end

    test "direct join" do
      test_permutations(
        [[~q(e), :name, ~q(name)], [~q(e2), :name, ~q(name)]],
        fn variant ->
          query = %{
            find: [~q(e), ~q(e2), ~q(name)],
            where: variant
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
      )
    end

    test "more direct joins" do
      test_permutations(
        [
          [~q(e), :name, ~q(name)],
          [~q(e), :age, ~q(age)],
          [~q(e), :height, ~q(height)]
        ],
        fn variant ->
          query = %{find: [~q(name), ~q(age), ~q(height)], where: variant}

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
      )
    end

    test "literal value match" do
      query = %{find: [~q(e)], where: [[~q(e), :name, "Marsha"]]}
      db = Db.new([%{name: "Marsha"}])
      result = Db.query(db, query)
      assert result == MapSet.new([%{e: 1}])
    end

    test "literal with var" do
      test_permutations(
        [[~q(e), :name, "Marsha"], [~q(e), :name, ~q(name)]],
        fn variant ->
          query = %{
            find: [~q(e), ~q(name)],
            where: variant
          }

          db = Db.new([%{name: "Marsha"}, %{name: "Bill should not appear"}])
          result = Db.query(db, query)

          assert result == MapSet.new([%{e: 1, name: "Marsha"}])
        end
      )
    end

    test "two literals with vars" do
      test_permutations(
        [
          [~q(e), :name, "Marsha"],
          [~q(e), :eye_color, "Blue"],
          [~q(e), :eye_color, ~q(eye_color)],
          [~q(e), :name, ~q(name)]
        ],
        fn variant ->
          query = %{find: [~q(e), ~q(name), ~q(eye_color)], where: variant}

          db =
            Db.new([
              %{name: "Marsha", eye_color: "Blue"},
              %{name: "Marsha", eye_color: "red should not be here"},
              %{name: "Bill should not appear"}
            ])

          result = Db.query(db, query)

          assert db.current_entity_id == 3

          assert result == MapSet.new([%{e: 1, eye_color: "Blue", name: "Marsha"}])
        end
      )
    end

    test "wildcard/blank/placeholder" do
      query = %{find: [~q(name)], where: [[:_, :name, ~q(name)]]}

      db =
        Db.new([
          %{name: "Bill", something_else: "nope"},
          %{no_name: "fine"},
          %{name: "Sandy", another: "ok"}
        ])

      result = Db.query(db, query)

      assert result == MapSet.new([%{name: "Bill"}, %{name: "Sandy"}])
    end

    test "wildcard/blank/placeholder with join" do
      test_permutations(
        [
          [~q(e), :name, "Bill"],
          [~q(e2), :friend_id, ~q(e)],
          [~q(e2), :favorite_color, :_],
          [~q(e2), :name, :_]
        ],
        fn variant ->
          data = [
            %{name: "Bill"},
            %{name: "Sue", friend_id: 1},
            %{name: "Johnny", friend_id: 1, favorite_color: :blue},
            %{name: "Juan", friend_id: 1},
            %{not_a_name: "ok", friend_id: 1, favorite_color: :green},
            %{name: "Megan", friend_id: 1, favorite_color: :red}
          ]

          # find friends of Bill's, with names, who have a favorite color,
          # but ignore what that color is
          query = %{find: [~q(e2)], where: variant}

          db = Db.new(data)
          result = Db.query(db, query)
          assert result == MapSet.new([%{e2: 3}, %{e2: 6}])
        end
      )
    end

    test "wildcard properly ignores fields in a find *" do
      test_permutations(
        [
          [~q(e), :name, "Bill"],
          [~q(e2), :friend_id, ~q(e)],
          [~q(e2), :favorite_color, :_],
          [~q(e2), :name, :_]
        ],
        fn variant ->
          data = [
            %{name: "Bill"},
            %{name: "Sue", friend_id: 1},
            %{name: "Johnny", friend_id: 1, favorite_color: :blue},
            %{name: "Juan", friend_id: 1},
            %{not_a_name: "ok", friend_id: 1, favorite_color: :green},
            %{name: "Megan", friend_id: 1, favorite_color: :red}
          ]

          # find friends of Bill's, with names, who have a favorite color,
          # but ignore what that color is
          query = %{find: [:*], where: variant}

          db = Db.new(data)
          result = Db.query(db, query)

          keys =
            Enum.reduce(result, MapSet.new(), fn row, acc ->
              keyset = Map.keys(row) |> MapSet.new()
              MapSet.union(acc, keyset)
            end)

          assert keys == MapSet.new([:e, :e2])
        end
      )
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

      assert db.current_entity_id == 1
      assert result == MapSet.new([%{e: 1, name: "Bill"}])
    end

    test "it does mutations with explicit elog/id" do
      db = Db.new()
      db = Db.transact(db, [%{"elog/id": 1, name: "Bill"}])
      db = Db.transact(db, [%{"elog/id": 1, name: "Jamie"}])
      db = Db.transact(db, [%{name: "Yolandi"}])
      db = Db.transact(db, [%{"elog/id": 2, name: "Jim"}])

      result =
        Db.query(db, %{
          find: [~q(e), ~q(name)],
          where: [[~q(e), :name, ~q(name)]]
        })

      assert db.current_entity_id == 2

      assert result == MapSet.new([%{e: 1, name: "Jamie"}, %{e: 2, name: "Jim"}])
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

      assert db.current_entity_id == 1
      assert result == MapSet.new([%{e: 1, name: "Jamie"}])
    end
  end

  describe "conditionals" do
    test "or working" do
      ors = [
        [~q(e), :eye_color, :blue],
        [~q(e), :eye_color, :green],
        [~q(e), :eye_color, :gray]
      ]

      test_permutations(ors, fn or_variant ->
        test_permutations(
          [
            [~q(e), :name, ~q(name)],
            {:or, or_variant},
            [~q(e), :eye_color, ~q(eye_color)]
          ],
          fn variant ->
            query = %{
              find: [~q(e), ~q(name), ~q(eye_color)],
              where: variant
            }

            db =
              Db.new([
                %{name: "Bill", eye_color: :blue},
                %{name: "May", eye_color: :green},
                %{name: "Millie", eye_color: :hazel},
                %{name: "Murph", eye_color: :gray}
              ])

            result = Db.query(db, query)

            assert result ==
                     MapSet.new([
                       %{e: 1, eye_color: :blue, name: "Bill"},
                       %{e: 2, eye_color: :green, name: "May"},
                       %{e: 4, name: "Murph", eye_color: :gray}
                     ])
          end
        )
      end)
    end

    test "or where vars don't equal" do
      ors = [
        [~q(e), :eye_color, :blue],
        [~q(not_equal), :eye_color, :green],
        [~q(e), :eye_color, :gray]
      ]

      test_permutations(ors, fn or_variant ->
        test_permutations(
          [
            {:or, or_variant},
            [~q(e), :name, ~q(name)]
          ],
          fn variant ->
            query = %{find: [~q(e), ~q(name), ~q(eye_color)], where: variant}

            db =
              Db.new([
                %{name: "Bill", eye_color: :blue},
                %{name: "May", eye_color: :green},
                %{name: "Millie", eye_color: :hazel},
                %{name: "Murph", eye_color: :gray}
              ])

            assert_raise RuntimeError,
                         fn ->
                           Db.query(db, query)
                         end
          end
        )
      end)
    end
  end
end
