defmodule Elog.Syntax do
  def sigil_q(s, []) do
    {:var, String.to_atom(s)}
  end
end
