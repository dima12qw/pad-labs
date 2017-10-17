defmodule ElixirPubsub.TopicRegistry do
  @name ElixirPubsub.TopicRegistry

  def start_link do
    Agent.start_link(fn -> Map.new end, name: @name)
  end

  def lookup(topic) do
    Agent.get(@name, fn dict ->
      Map.fetch(dict, topic)
    end)
  end

  def create(topic) do
    result = Agent.get(@name, fn dict -> Map.fetch(dict, topic) end)
    case result do
      :error ->
        {:ok, topic_agent} = ElixirPubsub.Topic.Supervisor.start_topic
        Agent.update(@name, fn dict -> Map.put(dict, topic, topic_agent) end)
        {:ok, topic_agent}
      {:ok, value} ->
        {:ok, value}
    end
  end

end
