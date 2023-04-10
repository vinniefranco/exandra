defmodule Exandra.MixProject do
  use Mix.Project

  @repo_url "https://github.com/vinniefranco/exandra"

  def project do
    [
      app: :exandra,
      version: "0.1.10",
      elixir: "~> 1.14",
      description:
        "Exandra is an Elixir library that brings the power of Scylla/Cassandra to Ecto. It is still in its very early stages and is not yet ready for production use. Exandra is an adapter for Ecto, allowing developers to use Scylla/Cassandra as a backend for their Elixir applications.",
      package: package(),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      test_coverage: [tool: ExCoveralls]
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp package do
    [
      maintainers: ["Vincent Franco"],
      licenses: ["MIT"],
      links: %{"GitHub" => @repo_url}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:ecto, "~> 3.9"},
      {:ecto_sql, "~> 3.9"},
      {:jason, "~> 1.4"},
      # DEV DEPS ------
      {:mox, "~> 1.0", only: :test},
      {:xandra, github: "vinniefranco/xandra", branch: "bump-deps"},
      {:credo, "~> 1.6", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.2", only: [:dev, :test], runtime: false},
      {:excoveralls, "~> 0.10", only: :test}
    ]
  end
end
