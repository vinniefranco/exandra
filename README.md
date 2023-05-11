# Exandra
[![CircleCI](https://dl.circleci.com/status-badge/img/gh/vinniefranco/exandra/tree/main.svg?style=svg)](https://dl.circleci.com/status-badge/redirect/gh/vinniefranco/exandra/tree/main)
[![Coverage Status](https://coveralls.io/repos/github/vinniefranco/exandra/badge.svg?branch=main)](https://coveralls.io/github/vinniefranco/exandra?branch=main)

Exandra is an Elixir library that brings the power of Scylla/Cassandra to Ecto. It is still in its very early stages and is not yet ready for production use. Exandra is an adapter for Ecto, allowing developers to use Scylla/Cassandra as a backend for their Elixir applications.

NOTE: This library is solely focused on integrating Ecto, and assumes Xandra is thoroughly tested.

## Installation

The package can be installed
by adding `exandra` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:exandra, "~> 0.1.140"}
  ]
end
```

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at <https://hexdocs.pm/exandra>.

