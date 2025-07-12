defmodule MapReduce.Application do
  @moduledoc false
  use Application

  @me __MODULE__
  @worker_pool_name :map_reduce_pool

  @impl true
  def start(_type, _args) do
    children = [
      MapReduce.TaskManager,
      worker_pool_spec()
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: @me)
  end

  def worker_pool_spec do
    cpus = :erlang.system_info(:logical_processors_available)

    poolboy_config = [
      name: {:local, @worker_pool_name},
      worker_module: MapReduce.Worker,
      size: cpus,
      max_overflow: cpus,
      strategy: :fifo
    ]

    :poolboy.child_spec(@worker_pool_name, poolboy_config)
  end
end
