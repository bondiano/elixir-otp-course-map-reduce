defmodule MapReduce.TaskManager do
  @moduledoc """
  Менеджер задач с простой логикой окон
  """
  use GenServer

  defstruct [
    :caller,
    :reducer_func,
    :window_size,
    :waiting_jobs,
    :active_tasks,
    :result
  ]

  def start_link({caller, jobs, reducer_func, window_size}) do
    GenServer.start_link(__MODULE__, {caller, jobs, reducer_func, window_size})
  end

  @impl true
  def init({caller, jobs, reducer_func, window_size}) do
    state = %__MODULE__{
      caller: caller,
      reducer_func: reducer_func,
      window_size: window_size,
      waiting_jobs: Enum.to_list(jobs),
      active_tasks: %{},
      result: nil
    }

    {:ok, fill_window(state)}
  end

  @impl true
  def handle_info({ref, result}, state) when is_reference(ref) do
    Process.demonitor(ref, [:flush])
    task_finished(ref, {:ok, result}, state)
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    task_finished(ref, {:error, reason}, state)
  end

  defp task_finished(ref, result, state) do
    new_active_tasks = Map.delete(state.active_tasks, ref)

    new_result =
      case result do
        {:ok, value} ->
          combine_result(state.result, value, state.reducer_func)

        {:error, _} ->
          state.result
      end

    new_state = %__MODULE__{state | active_tasks: new_active_tasks, result: new_result}

    if finished?(new_state) do
      send(new_state.caller, {:reduce_completed, new_result})
      {:stop, :normal, new_state}
    else
      final_state = fill_window(new_state)
      {:noreply, final_state}
    end
  end

  defp fill_window(state) do
    free_slots = state.window_size - map_size(state.active_tasks)
    {jobs_to_run, remaining_jobs} = Enum.split(state.waiting_jobs, free_slots)

    new_active_tasks =
      Enum.reduce(jobs_to_run, state.active_tasks, fn job, acc ->
        task = start_job_task(job)

        Map.put(acc, task.ref, task)
      end)

    %__MODULE__{state | waiting_jobs: remaining_jobs, active_tasks: new_active_tasks}
  end

  defp start_job_task(job) do
    Task.async(fn ->
      :poolboy.transaction(:worker_pool, fn worker ->
        GenServer.call(worker, {:execute, job})
      end)
    end)
  end

  defp finished?(state) do
    state.waiting_jobs == [] and map_size(state.active_tasks) == 0
  end

  defp combine_result(nil, new_value, _), do: new_value

  defp combine_result(current, new_value, reducer_func) do
    try do
      reducer_func.(current, new_value)
    rescue
      _ -> current
    end
  end
end
