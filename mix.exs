defmodule Elog.MixProject do
  use Mix.Project

  def project do
    [
      app: :elog,
      version: "0.1.0",
      elixir: "~> 1.7",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {Elog.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:strand, "~> 0.5"},
      {:bimap, "~> 1.0"},
      {:benchee, "~> 0.13.2", only: :dev}
    ]
  end
end
