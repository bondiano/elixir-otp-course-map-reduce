defmodule MapReduce.TaskManager do
  @moduledoc false
  use DynamicSupervisor

  @me __MODULE__

  def start_link(_) do
    DynamicSupervisor.start_link(__MODULE__, nil, name: @me)
  end

  @impl true
  def init(_args) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end

  def create(opts \\ [timeout: 5000]) do
    DynamicSupervisor.start_child(@me, {MapReduce.Task, opts})
  end

  def execute(task, func) do
    GenServer.cast(task, {:execute, func})
  end

  def get_result(task) do
    GenServer.call(task, {:get_result})
  end

  def get_status(task) do
    GenServer.call(task, {:get_status})
  end

  def reduce(functions, aggregate_func) do
    tasks = Enum.map(functions, fn func -> execute(create(), func) end)
    results = Enum.map(tasks, &get_result(&1))

    Enum.reduce(results, aggregate_func)
  end
end
