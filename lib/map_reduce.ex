defmodule MapReduce do
  @moduledoc """
  MapReduce is a library for distributed task execution.
  """

  @doc """
  Запускает несколько задач и применяет к результатам ассоциативную и коммутативную агрегатную функцию
  """
  @spec reduce(Enumerable.t(), (any(), any() -> any()), Keyword.t()) ::
          {:ok, any()} | {:error, any()}
  def reduce(jobs, reducer_func, opts \\ [])
      when is_function(reducer_func, 2) do
    window_size = Keyword.get(opts, :window_size, 100)
    timeout = Keyword.get(opts, :timeout, 60_000)
    stop_on_error = Keyword.get(opts, :stop_on_error, false)

    caller = self()

    task_manager_spec = %{
      id: MapReduce.TaskManager,
      start:
        {MapReduce.TaskManager, :start_link,
         [
           [
             {:caller, caller},
             {:jobs, jobs},
             {:reducer_func, reducer_func},
             {:window_size, window_size},
             {:timeout, timeout},
             {:stop_on_error, stop_on_error}
           ]
         ]},
      restart: :temporary
    }

    case DynamicSupervisor.start_child(MapReduce.TaskManagerSupervisor, task_manager_spec) do
      {:ok, manager_pid} ->
        Process.flag(:trap_exit, true)
        Process.link(manager_pid)

        receive do
          {:reduce_completed, result} ->
            Process.unlink(manager_pid)
            {:ok, result}

          {:reduce_failed, reason} ->
            Process.unlink(manager_pid)
            {:error, reason}

          {:EXIT, ^manager_pid, reason} ->
            {:error, {:manager_died, reason}}
        after
          timeout ->
            DynamicSupervisor.terminate_child(MapReduce.TaskManagerSupervisor, manager_pid)
            {:error, :timeout}
        end

      {:error, reason} ->
        {:error, {:failed_to_start_manager, reason}}
    end
  end

  @doc """
  Получает статистику пула воркеров
  """
  def pool_stats do
    :poolboy.status(:worker_pool)
  end
end
