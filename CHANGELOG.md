# Changelog

## v0.17.0

  * Add support for `inc`, `push`, and `pull` update expressions in `update_all`.

## v0.16.0

  * Update the `:ecto` and `:ecto_sql` requirements to `~> 3.13`.
  * Update the `:xandra` requirement to `~> 0.19.4`.

## v0.15.0

  * Support options for updates in `update_all`.

## v0.14.0

  * Rename partition options to cluster options.

## v0.13.0

  * Support multiple updates in `update_all`.
  * Support options for insertions.
  * Fix tuple `nil` casting.
  * Fix UUID types in data structures.
  * Properly define `embed_as/2` for custom types.
  * Add missing typespec `t` types.

## v0.11.0

  * Update the `:ecto` requirement to `~> 3.12`.

## v0.10.3

  * Add `Exandra.Tuple` type.
  * Add `Exandra.Inet` type.
  * Add support for page streaming from `query/4`.
  * Support composite/custom types in type parameters.
  * Fix `load`/`dump` Ecto function calls for custom types.
  * Update the `:xandra` requirement to `~> 0.19`.

## v0.10.2

  * Fix a bug with index creation/deletions in migrations

## v0.10.1

  * Fix a bug with `nil` decimals that happens on Ecto 3.11+.

## v0.10.0

  * Update the `:xandra` requirement to `~> 0.18.0`.

## v0.9.1

  * Fix issues with migrations generating invalid CQL statements.

## v0.9.0

  * Add the `Exandra.Counter.t/0` type.

## v0.8.1

  * Fix issue with types in map column.

## v0.8.0

  * Add support for multiple keyspaces.

## v0.6.5

  * Update the `:xandra` requirement to `~> 0.17.0`.
