defmodule KV.Router do
  require Logger

  @default_routing_table [{1..120, :this}]

  @doc """
  Dispatch the given `mod`, `fun`, `args` request
  to the appropriate node based on the `topic`.
  """
  def route(bucket, mod, fun, args) do
    # Get the first byte of the binary.
    first = :binary.first(bucket)

    # Try to find an entry in the table or raise
    entry = filter_entries(bucket, first, table())

    Logger.info("Routing operations for '#{bucket}' to '#{inspect elem(entry, 1)}'.")

    # If the entry node is the current node.
    result = case elem(entry, 1) do
      :this ->
        apply(mod, fun, args)

      value when value == node() ->
        apply(mod, fun, args)

      _ ->
        {KV.RouterTasks, elem(entry, 1)}
        |> Task.Supervisor.async(KV.Router, :route, [bucket, mod, fun, args])
        |> Task.await()
    end

    {first, result}
  end

  defp filter_entries(bucket, first, routing_table) do
    Enum.find(routing_table, fn ({enum, _node}) -> first in enum end) || no_entry_error(bucket)
  end

  defp no_entry_error(bucket) do
    raise "could not find entry for #{inspect bucket} in table #{inspect table()}"
  end

  @doc """
  The routing table.
  """
  def table() do
    table(Application.get_env(:kv, :routing_table))
  end

  defp table(nil), do: @default_routing_table
  defp table([]), do: @default_routing_table
  defp table(value), do: value
end
