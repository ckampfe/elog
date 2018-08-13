alias Elog.Db
import Elog.Syntax

simple =
  Stream.cycle([%{name: "Bill"}, %{name: "Bob"}, %{name: "Grendel"}])

complex =
  Stream.cycle([%{name: "Bill",
                  age: 23,
                  height: 723,
                  interests: ["Squash", "Candy", "great old movies"],
                  marital_status: "married to the game"},
                %{name: "Sandy",
                  immigration_status: "doesn't matter",
                  other_crap: %{a: 1, b: 2, c: 3},
                  height: 4,
                  favorite_band: "Wu tang"},
                %{favorite_band: "Juve",
                  favorite_soccer: "Juve",
                  favorite_color: "red",
                  favorite_candy: "reeses",
                  name: "Eleanor", age: 92}])

small = 100
medium = 1_000
large = 10_000

small_simple = Enum.take(simple, small)
medium_simple = Enum.take(simple, medium)
large_simple = Enum.take(simple, large)

small_complex = Enum.take(complex, small)
medium_complex = Enum.take(complex, medium)
large_complex = Enum.take(complex, large)

simple_variable_find = %{find: [~q(e), ~q(name)], where: [[~q(e), :name, ~q(name)]]}
simple_literal_find = %{find: [~q(e)], where: [[~q(e), :name, "Bob"]]}
simple_literal_find_join_literal_first = %{find: [~q(e), ~q(name)], where: [[~q(e), :name, "Bob"], [~q(e), :name, ~q(name)]]}
simple_literal_find_join_join_first = %{find: [~q(e), ~q(name)], where: [[~q(e), :name, ~q(name)], [~q(e), :name, "Bob"]]}

small_simple_db = Db.new(small_simple)
medium_simple_db = Db.new(medium_simple)
large_simple_db = Db.new(large_simple)

small_complex_db = Db.new(small_complex)
medium_complex_db = Db.new(medium_complex)
large_complex_db = Db.new(large_complex)

Benchee.run(%{
  "small simple variable find" => fn -> Db.query(small_simple_db, simple_variable_find) end,
  "medium simple variable find" => fn -> Db.query(medium_simple_db, simple_variable_find) end,
  # "large simple variable find" => fn -> Db.query(large_simple_db, simple_variable_find) end,

  "small simple literal find" => fn -> Db.query(small_simple_db, simple_literal_find) end,
  "medium simple literal find" => fn -> Db.query(medium_simple_db, simple_literal_find) end,
  # "large simple literal find" => fn -> Db.query(large_simple_db, simple_literal_find) end,

  "small simple literal find join literal first" => fn -> Db.query(small_simple_db, simple_literal_find_join_literal_first) end,
  "medium simple literal find join literal first" => fn -> Db.query(medium_simple_db, simple_literal_find_join_literal_first) end,

  # "large simple literal find join literal first" => fn -> Db.query(large_simple_db, simple_literal_find_join_literal_first) end,

  "small simple literal find join, join first" => fn -> Db.query(small_simple_db, simple_literal_find_join_join_first) end,
  "medium simple literal find join, join first" => fn -> Db.query(medium_simple_db, simple_literal_find_join_join_first) end,

  # "large simple literal find join join first" => fn -> Db.query(large_simple_db, simple_literal_find_join_join_first) end,

  "small complex find" => fn -> Db.query(small_complex_db, simple_variable_find) end,
  "medium complex find" => fn -> Db.query(medium_complex_db, simple_variable_find) end,

}, time: 10, memory_time: 2)
