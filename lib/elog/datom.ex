defmodule Elog.Datom do
  require Record
  Record.defrecord(:datom, [:e, :a, :v, :t])
end
