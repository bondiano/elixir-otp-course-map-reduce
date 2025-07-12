defmodule MapReduce.Application do
  @moduledoc false
  use Application

  @impl true
  def start(_, _) do
    children = [
      poolboy_spec(),
      MapReduce.TaskManagerSupervisor
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: MapReduce.ApplicationSupervisor)
  end

  def poolboy_spec do
    cpus = System.schedulers_online()

    poolboy_config = [
      name: {:local, :worker_pool},
      worker_module: MapReduce.Worker,
      size: cpus,
      max_overflow: cpus * 2,
      strategy: :lifo
    ]

    :poolboy.child_spec(:worker_pool, poolboy_config, [])
  end
end
