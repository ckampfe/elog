defmodule ElogDbAggregatesTest do
  use ExUnit.Case, async: true
  alias Elog.Db
  import Elog.Syntax
  import TestHelper

  describe "aggregates" do
    test "it does count with no rename" do
      query = %{
        find: [~q(name), {:count, ~q(e)}],
        where: [[~q(e), :name, ~q(name)]]
      }

      db =
        Db.new([
          %{name: "Marsha", age: 7, size: 98.0},
          %{name: "Marsha", age: 742, size: 9.0},
          %{name: "Christopher", age: 63},
          %{name: "Christopher", age: 21}
        ])

      result = Db.query(db, query)

      assert result ==
               MapSet.new([
                 %{name: "Christopher", count: 2},
                 %{name: "Marsha", count: 2}
               ])
    end

    test "it does count with rename" do
      query = %{
        find: [~q(name), {:count, ~q(e), :count_with_different_name}],
        where: [[~q(e), :name, ~q(name)]]
      }

      db =
        Db.new([
          %{name: "Marsha", age: 7, size: 98.0},
          %{name: "Marsha", age: 742, size: 9.0},
          %{name: "Christopher", age: 63},
          %{name: "Christopher", age: 21}
        ])

      result = Db.query(db, query)

      assert result ==
               MapSet.new([
                 %{name: "Christopher", count_with_different_name: 2},
                 %{name: "Marsha", count_with_different_name: 2}
               ])
    end

    test "it does sum with rename" do
      test_permutations(
        [
          [~q(e), :name, ~q(name)],
          [~q(e), :age, ~q(age)]
        ],
        fn variant ->
          query = %{
            find: [~q(name), {:sum, ~q(age), :sum_age}],
            where: variant
          }

          db =
            Db.new([
              %{name: "Marsha", age: 7, size: 98.0},
              %{name: "Marsha", age: 742, size: 9.0},
              %{name: "Christopher", age: 63},
              %{name: "Christopher", age: 21}
            ])

          result = Db.query(db, query)

          assert result ==
                   MapSet.new([
                     %{name: "Christopher", sum_age: 84},
                     %{name: "Marsha", sum_age: 749}
                   ])
        end
      )
    end

    test "it does sum with no rename" do
      test_permutations(
        [
          [~q(e), :name, ~q(name)],
          [~q(e), :age, ~q(age)]
        ],
        fn variant ->
          query = %{
            find: [~q(name), {:sum, ~q(age)}],
            where: variant
          }

          db =
            Db.new([
              %{name: "Marsha", age: 7, size: 98.0},
              %{name: "Marsha", age: 742, size: 9.0},
              %{name: "Christopher", age: 63},
              %{name: "Christopher", age: 21}
            ])

          result = Db.query(db, query)

          assert result ==
                   MapSet.new([
                     %{name: "Christopher", sum: 84},
                     %{name: "Marsha", sum: 749}
                   ])
        end
      )
    end

    test "it does min with rename" do
      test_permutations(
        [
          [~q(e), :name, ~q(name)],
          [~q(e), :age, ~q(age)]
        ],
        fn variant ->
          query = %{find: [~q(name), {:min, ~q(age), :min_age}], where: variant}

          db =
            Db.new([
              %{name: "Marsha", age: 7, size: 98.0},
              %{name: "Marsha", age: 742, size: 9.0},
              %{name: "Christopher", age: 63},
              %{name: "Christopher", age: 21}
            ])

          result = Db.query(db, query)

          assert result ==
                   MapSet.new([
                     %{name: "Christopher", min_age: 21},
                     %{name: "Marsha", min_age: 7}
                   ])
        end
      )
    end

    test "it does min with no rename" do
      test_permutations(
        [
          [~q(e), :name, ~q(name)],
          [~q(e), :age, ~q(age)]
        ],
        fn variant ->
          query = %{find: [~q(name), {:min, ~q(age)}], where: variant}

          db =
            Db.new([
              %{name: "Marsha", age: 7, size: 98.0},
              %{name: "Marsha", age: 742, size: 9.0},
              %{name: "Christopher", age: 63},
              %{name: "Christopher", age: 21}
            ])

          result = Db.query(db, query)

          assert result ==
                   MapSet.new([
                     %{name: "Christopher", min: 21},
                     %{name: "Marsha", min: 7}
                   ])
        end
      )
    end

    test "it does max with rename" do
      test_permutations(
        [
          [~q(e), :name, ~q(name)],
          [~q(e), :age, ~q(age)]
        ],
        fn variant ->
          query = %{find: [~q(name), {:max, ~q(age), :max_age}], where: variant}

          db =
            Db.new([
              %{name: "Marsha", age: 7, size: 98.0},
              %{name: "Marsha", age: 742, size: 9.0},
              %{name: "Christopher", age: 63},
              %{name: "Christopher", age: 21}
            ])

          result = Db.query(db, query)

          assert result ==
                   MapSet.new([
                     %{name: "Christopher", max_age: 63},
                     %{name: "Marsha", max_age: 742}
                   ])
        end
      )
    end

    test "it does max with no rename" do
      test_permutations(
        [
          [~q(e), :name, ~q(name)],
          [~q(e), :age, ~q(age)]
        ],
        fn variant ->
          query = %{find: [~q(name), {:max, ~q(age)}], where: variant}

          db =
            Db.new([
              %{name: "Marsha", age: 7, size: 98.0},
              %{name: "Marsha", age: 742, size: 9.0},
              %{name: "Christopher", age: 63},
              %{name: "Christopher", age: 21}
            ])

          result = Db.query(db, query)

          assert result ==
                   MapSet.new([
                     %{name: "Christopher", max: 63},
                     %{name: "Marsha", max: 742}
                   ])
        end
      )
    end

    test "it does averages with no rename" do
      test_permutations(
        [
          [~q(e), :age, ~q(age)],
          [~q(e), :name, ~q(name)]
        ],
        fn variant ->
          query = %{find: [~q(name), {:avg, ~q(age)}], where: variant}

          db =
            Db.new([
              %{name: "Marsha", age: 7, size: 98.0},
              %{name: "Marsha", age: 742, size: 9.0},
              %{name: "Christopher", age: 63},
              %{name: "Christopher", age: 21}
            ])

          result = Db.query(db, query)

          assert result ==
                   MapSet.new([
                     %{name: "Christopher", avg: 42.0},
                     %{name: "Marsha", avg: 374.5}
                   ])
        end
      )
    end

    test "it does averages with rename" do
      test_permutations(
        [
          [~q(e), :age, ~q(age)],
          [~q(e), :name, ~q(name)]
        ],
        fn variant ->
          query = %{
            find: [~q(name), {:avg, ~q(age), :average_age}],
            where: variant
          }

          db =
            Db.new([
              %{name: "Marsha", age: 7, size: 98.0},
              %{name: "Marsha", age: 742, size: 9.0},
              %{name: "Christopher", age: 63},
              %{name: "Christopher", age: 21}
            ])

          result = Db.query(db, query)

          assert result ==
                   MapSet.new([
                     %{name: "Christopher", average_age: 42.0},
                     %{name: "Marsha", average_age: 374.5}
                   ])
        end
      )
    end
  end

  describe "multiple aggregates" do
    test "multiple aggregates" do
      test_permutations(
        [
          [~q(e), :age, ~q(age)],
          [~q(e), :name, ~q(name)]
        ],
        fn variant ->
          query = %{
            find: [
              ~q(name),
              {:avg, ~q(age), :average_age},
              {:max, ~q(age), :max_age},
              {:count, ~q(e)}
            ],
            where: variant
          }

          db =
            Db.new([
              %{name: "Marsha", age: 7, size: 98.0},
              %{name: "Marsha", age: 742, size: 9.0},
              %{name: "Christopher", age: 63},
              %{name: "Christopher", age: 21}
            ])

          result = Db.query(db, query)

          assert result ==
                   MapSet.new([
                     %{
                       name: "Christopher",
                       average_age: 42.0,
                       max_age: 63,
                       count: 2
                     },
                     %{
                       name: "Marsha",
                       average_age: 374.5,
                       max_age: 742,
                       count: 2
                     }
                   ])
        end
      )
    end

    test "multiple group by variables" do
      test_permutations(
        [
          [~q(e), :age, ~q(age)],
          [~q(e), :day, ~q(day)],
          [~q(e), :name, ~q(name)]
        ],
        fn variant ->
          query = %{
            find: [
              ~q(name),
              ~q(day),
              {:avg, ~q(age), :average_age},
              {:count, ~q(e), :count}
            ],
            where: variant
          }

          db =
            Db.new([
              %{name: "Marsha", day: 1, age: 99, size: 98.0},
              %{name: "Marsha", day: 1, age: 19, size: 9.0},
              %{name: "Marsha", day: 3, age: 102, size: 9.0},
              %{name: "Marsha", day: 3, age: 103, size: 9.0},
              %{name: "Christopher", day: 4, age: 9},
              %{name: "Christopher", day: 2, age: 63},
              %{name: "Christopher", day: 2, age: 21}
            ])

          result = Db.query(db, query)

          assert result ==
                   MapSet.new([
                     %{name: "Christopher", day: 4, average_age: 9.0, count: 1},
                     %{
                       name: "Christopher",
                       day: 2,
                       average_age: 42.0,
                       count: 2
                     },
                     %{name: "Marsha", day: 1, average_age: 59.0, count: 2},
                     %{name: "Marsha", day: 3, average_age: 102.5, count: 2}
                   ])
        end
      )
    end
  end
end
