defmodule Exandra.MixProject do
  use Mix.Project

  @repo_url "https://github.com/vinniefranco/exandra"

  @description """
  Exandra is an Elixir library that brings the power of Scylla/Cassandra to Ecto.
  """

  def project do
    [
      app: :exandra,
      version: "0.6.4",
      elixir: "~> 1.14",
      description: @description,
      package: package(),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      elixirc_paths: elixirc_paths(Mix.env()),
      test_coverage: [tool: ExCoveralls],
      preferred_cli_env: [
        "test.cassandra": :test,
        "test.scylla": :test,
        "test.all": :test,
        "coveralls.html": :test
      ],
      aliases: aliases(),
      xref: [exclude: [XandraClusterMock, XandraMock]],
      docs: [
        main: "Exandra",
        groups_for_modules: [
          "Ecto types": [Exandra.UDT, Exandra.Counter, Exandra.Map, Exandra.Set]
        ]
      ]
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: []
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_other), do: ["lib"]

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
      {:decimal, "~> 1.6 or ~> 2.0"},
      {:ecto, "~> 3.10"},
      {:ecto_sql, "~> 3.10"},
      {:jason, "~> 1.4"},
      {:nimble_options, "~> 1.0"},
      {:xandra, "~> 0.17.0"},

      # DEV DEPS ------
      {:mox, "~> 1.0", only: :test},
      {:credo, "~> 1.6", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.2", only: [:dev, :test], runtime: false},
      {:excoveralls, "~> 0.16.1", only: :test},
      {:ex_doc, "~> 0.29", only: :dev, runtime: false}
    ]
  end

  defp aliases do
    [
      "test.cassandra": [&print_tests_banner(&1, :cassandra), "test"],
      "test.scylla": [&print_tests_banner(&1, :scylla), "cmd EXANDRA_PORT=9043 mix test --color"],
      "test.all": fn args ->
        Mix.Task.run(:"test.cassandra", args)
        Mix.Task.run(:"test.scylla", args)
      end
    ]
  end

  defp print_tests_banner(_argv, db) do
    IO.puts(IO.ANSI.format([:bright, :cyan, "==> Running tests for #{db}"]))
  end
end
