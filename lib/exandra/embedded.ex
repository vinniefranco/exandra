defmodule Exandra.Embedded do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      import Exandra.Embedded
    end
  end

  defmacro embedded_type(field, embedded_schema, opts \\ []) do
    quote do
      opts = Keyword.merge([using: unquote(embedded_schema)], unquote(opts))
      field unquote(field), Exandra.EmbeddedType, opts
    end
  end
end
