defmodule Exandra.MixProject do
  use Mix.Project

  @repo_url "https://github.com/vinniefranco/exandra"

  def project do
    [
      app: :exandra,
      version: "0.1.0",
      elixir: "~> 1.14",
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
