defmodule MapReduce.Application do
  @moduledoc false
  use Application

  @me __MODULE__

  @impl true
  def start(_, _) do
    children = [
      poolboy_spec(),
      MapReduce.TaskManagerSupervisor
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: @me)
  end

  def poolboy_spec do
    cpus = :erlang.system_info(:logical_processors_available)

    poolboy_config = [
      name: {:local, :worker_pool},
      worker_module: MapReduce.Worker,
      size: cpus,
      max_overflow: cpus
    ]

    :poolboy.child_spec(:worker_pool, poolboy_config, [])
  end
end
