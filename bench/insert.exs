alias Elog.Db

simple = Stream.cycle([%{name: "Bill"}, %{name: "Bob"}, %{name: "Grendel"}])

complex =
  Stream.cycle([
    %{
      name: "Bill",
      age: 23,
      height: 723,
      interests: ["Squash", "Candy", "great old movies"],
      marital_status: "married to the game"
    },
    %{
      name: "Sandy",
      immigration_status: "doesn't matter",
      other_crap: %{a: 1, b: 2, c: 3},
      height: 4,
      favorite_band: "Wu tang"
    },
    %{
      favorite_band: "Juve",
      favorite_soccer: "Juve",
      favorite_color: "red",
      favorite_candy: "reeses",
      name: "Eleanor",
      age: 92
    }
  ])

tiny = 10
small = 1000
medium = 100_000
large = 1_000_000

small_simple = Enum.take(simple, small)
medium_simple = Enum.take(simple, medium)
large_simple = Enum.take(simple, large)

small_complex = Enum.take(complex, small)
medium_complex = Enum.take(complex, medium)
large_complex = Enum.take(complex, large)

example_small_db = Db.new(small_simple)
example_medium_db = Db.new(medium_simple)
example_large_db = Db.new(large_simple)
example_small_db_no_avet = Db.new(small_simple, %{indexes: [:eavt, :aevt]})
example_medium_db_no_avet = Db.new(medium_simple, %{indexes: [:eavt, :aevt]})
example_large_db_no_avet = Db.new(large_simple, %{indexes: [:eavt, :aevt]})

tiny_insert = Enum.take(simple, tiny)

Benchee.run(
  %{
    "new small simple" => fn -> Db.new(small_simple) end,
    "new medium simple" => fn -> Db.new(medium_simple) end,
    # "new large simple" => fn -> Db.new(large_simple) end,
    "new small complex" => fn -> Db.new(small_complex) end,
    "new medium complex" => fn -> Db.new(medium_complex) end,
    # "new large complex" => fn -> Db.new(large_complex) end,
    "new small simple no avet" => fn ->
      Db.new(small_simple, %{indexes: [:eavt, :aevt]})
    end,
    "new medium simple no avet" => fn ->
      Db.new(medium_simple, %{indexes: [:eavt, :aevt]})
    end,
    "transact tiny - small simple" => fn ->
      Db.transact(example_small_db, tiny_insert)
    end,
    "transact tiny - medium simple" => fn ->
      Db.transact(example_medium_db, tiny_insert)
    end,
    "transact tiny - large simple" => fn ->
      Db.transact(example_large_db, tiny_insert)
    end,
    "transact tiny - small simple no avet" => fn ->
      Db.transact(example_small_db_no_avet, tiny_insert)
    end,
    "transact tiny - medium simple no avet" => fn ->
      Db.transact(example_medium_db_no_avet, tiny_insert)
    end,
    "transact tiny - large simple no avet" => fn ->
      Db.transact(example_large_db_no_avet, tiny_insert)
    end
  },
  time: 5,
  memory_time: 2
)
