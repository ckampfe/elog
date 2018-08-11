alias Elog.Db

simple =
  Stream.cycle([%{name: "Bill"}, %{name: "Bob"}, %{name: "Grendel"}])

complex =
  Stream.cycle([%{name: "Bill", age: 23, height: 723, interests: ["Squash", "Candy", "great old movies"], marital_status: "married to the game"},
                %{name: "Sandy", immigration_status: "doesn't matter", other_crap: %{a: 1, b: 2, c: 3}, height: 4, favorite_band: "Wu tang"},
               %{favorite_band: "Juve", favorite_soccer: "Juve", favorite_color: "red", favorite_candy: "reeses", name: "Eleanor", age: 92}])

small = 1000
medium = 100_000
large = 1_000_000

small_simple = Enum.take(simple, small)
medium_simple = Enum.take(simple, medium)
large_simple = Enum.take(simple, large)

small_complex = Enum.take(complex, small)
medium_complex = Enum.take(complex, medium)
large_complex = Enum.take(complex, large)

Benchee.run(%{
  "small simple"    => fn -> Db.new(small_simple) end,
  "medium simple" => fn -> Db.new(medium_simple) end,
  "large simple" => fn -> Db.new(large_simple) end,
  "small complex" => fn -> Db.new(small_complex) end,
  "medium complex" => fn -> Db.new(medium_complex) end,
  "large complex" => fn -> Db.new(large_complex) end,
}, time: 10, memory_time: 2)
