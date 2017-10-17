defmodule KV.Server.Command do
  @separator ","
  @routable_commands [:create, :del, :keys, :get, :put]

  @doc ~S"""
  Parses the given `line` into a command.


  """
  def parse(line) do
    case String.split(line) do
      ["CREATE", topic] -> {:ok, {:create, topic}}
#      ["DELETE", bucket] -> {:ok, {:del, encode(bucket)}}
      ["GET_ALL", topic] -> {:ok, {:keys, topic}}
      ["GET", topic] -> {:ok, {:get, topic}}
      ["PUT", topic, message] -> {:ok, {:put, topic, message}}
#      ["DELETE", bucket, key] -> {:ok, {:del, encode(bucket), encode(key)}}   ["SUM", bucket] -> {:ok, {:sum, encode(bucket)}}

      ["BUCKETS"] -> {:ok, :buckets}
      _ -> {:error, :unknown_command}
    end
  end

  @doc """
  Runs the given command and when needed - also routes it to proper node.
  """
    def run(command) when elem(command, 0) in @routable_commands do
      {first, result} = KV.Router.route(elem(command, 1), KV.Server.Command, :execute, [ command ])
      KV.Router.Statistics.collect(KV.Router.Statistics, first)
      result
    end

  def run(command) do
    KV.Server.Command.execute(command)
  end

  @doc """
  Actually execute without persistence the command.
  """
  def execute_without_persistence(command) do
    invoke(command)
  end

  @doc """
  Actually executes the command.
  """
  def execute(command) do
    do_when_persistence_enabled(fn () -> KV.Persistence.store(command) end)
    invoke(command)
  end

  defp invoke({:create, topic}) do
    ElixirPubsub.TopicRegistry.create(topic)
    {:ok, "OK\r\n"}
  end



  defp invoke({:keys, topic}) do
    {:ok, topic_agent} = ElixirPubsub.TopicRegistry.lookup(topic)
    case ElixirPubsub.Topic.pop_all(topic_agent) do
      {:ok, value} -> {:ok, "#{value}\r\nOK\r\n"}
      :error -> {:ok, "Topic is empty\r\n"} end
  end

  defp invoke({:get, topic}) do
    {:ok, topic_agent} = ElixirPubsub.TopicRegistry.lookup(topic)
    case ElixirPubsub.Topic.pop(topic_agent) do
      {:ok, value} -> {:ok, "#{value}\r\nOK\r\n"}
      :error -> {:ok, "Topic is empty\r\nOK\r\n"} end
  end

  defp invoke({:put, topic, message}) do
    {:ok, topic_agent} = ElixirPubsub.TopicRegistry.create(topic)
    ElixirPubsub.Topic.push(topic_agent, message)
    {:ok, "OK\r\n"}
  end





  # Private API.


  defp do_when_persistence_enabled(action) do
    do_when_persistence_enabled(Application.get_env(:kv_server, :persistence_enabled), action)
  end

  defp do_when_persistence_enabled(true, action), do: action.()
  defp do_when_persistence_enabled(false, _action), do: nil


end
