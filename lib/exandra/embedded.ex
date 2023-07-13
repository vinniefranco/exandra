defmodule Exandra.Embedded do
  @moduledoc false

  alias __MODULE__

  defmacro __using__(_opts) do
    quote do
      import Exandra.Embedded

      Module.register_attribute(__MODULE__, :__exandra_types__, accumulate: true)
    end
  end

  defmacro __exandra_types__ do
    quote do
      @__exandra_types__
    end
  end

  defmacro cast_type(changeset, field, params) do
    quote do
      embeds = unquote(__MODULE__).__exandra_types__()

      Embedded.attempt_cast_and_coerce(
        unquote(changeset),
        unquote(field),
        unquote(params),
        embeds
      )
    end
  end

  defmacro embedded_type(field, embedded_schema, opts \\ []) do
    quote do
      Module.put_attribute(
        __MODULE__,
        :__exandra_types__,
        {unquote(field), unquote(embedded_schema), unquote(opts)}
      )

      field unquote(field), Exandra.EmbeddedType, into: unquote(embedded_schema)
    end
  end

  def attempt_cast_and_coerce(changeset, field, params, embeds) do
    Enum.reduce(embeds, changeset, fn {embedded_field, schema, _opts}, changeset ->
      if field == embedded_field do
        case apply_cast(schema, params) do
          {:ok, struct} ->
            Ecto.Changeset.put_change(changeset, embedded_field, struct)

          {:error, _error_changeset} ->
            Ecto.Changeset.add_error(changeset, field, "cannot be cast")
        end
      else
        changeset
      end
    end)
  end

  defp apply_cast(schema, params) do
    schema
    |> struct()
    |> schema.changeset(params)
    |> Ecto.Changeset.apply_action(:insert)
  end
end
