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
    :active_workers,
    :result,
    :stop_on_error,
    :completed_results,
    :next_job_index
  ]

  @type t :: %__MODULE__{
          caller: pid(),
          reducer_func: (any(), any() -> any()),
          window_size: pos_integer(),
          waiting_jobs: [{any(), non_neg_integer()}],
          active_workers: %{reference() => {pid(), non_neg_integer()}},
          result: any(),
          stop_on_error: boolean(),
          completed_results: %{non_neg_integer() => any()},
          next_job_index: non_neg_integer()
        }

  @spec start_link(Keyword.t()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @spec init(Keyword.t()) :: {:ok, %__MODULE__{}}
  def init(opts) do
    caller = Keyword.fetch!(opts, :caller)
    jobs = Keyword.fetch!(opts, :jobs)
    reducer_func = Keyword.fetch!(opts, :reducer_func)
    window_size = Keyword.fetch!(opts, :window_size)
    stop_on_error = Keyword.get(opts, :stop_on_error, false)

    indexed_jobs = jobs |> Enum.to_list() |> Enum.with_index()

    state = %__MODULE__{
      caller: caller,
      reducer_func: reducer_func,
      window_size: window_size,
      waiting_jobs: indexed_jobs,
      active_workers: %{},
      stop_on_error: stop_on_error,
      result: nil,
      completed_results: %{},
      next_job_index: 0
    }

    if Enum.empty?(state.waiting_jobs) do
      send(caller, {:reduce_completed, nil})
      {:ok, state, {:continue, :stop}}
    else
      {:ok, fill_window(state)}
    end
  end

  def handle_info({:DOWN, _ref, :process, worker, reason}, state) do
    {job_index, active_workers} =
      Enum.reduce(state.active_workers, {nil, %{}}, fn
        {_key, {^worker, index}}, {_, acc} -> {index, acc}
        {key, value}, {found_index, acc} -> {found_index, Map.put(acc, key, value)}
      end)

    task_finished(active_workers, {:error, reason}, job_index, state)
  end

  def handle_info({[:alias | req_id], result}, state) do
    {{worker, job_index}, active_workers} = Map.pop!(state.active_workers, req_id)
    :poolboy.checkin(:worker_pool, worker)
    task_finished(active_workers, result, job_index, state)
  end

  defp task_finished(new_active_workers, result, job_index, state) do
    case result do
      {:ok, value} ->
        handle_successful_result(new_active_workers, value, job_index, state)

      {:error, error} ->
        handle_error_result(new_active_workers, error, state)
    end
  end

  defp handle_successful_result(new_active_workers, value, job_index, state) do
    temp_completed_results = Map.put(state.completed_results, job_index, value)

    {new_result, new_next_index} =
      process_completed_results(
        temp_completed_results,
        state.next_job_index,
        state.result,
        state.reducer_func
      )

    new_completed_results =
      cleanup_completed_results(
        temp_completed_results,
        state.next_job_index,
        new_next_index
      )

    new_state = %__MODULE__{
      state
      | active_workers: new_active_workers,
        result: new_result,
        completed_results: new_completed_results,
        next_job_index: new_next_index
    }

    next_job(new_state, new_result)
  end

  defp handle_error_result(new_active_workers, error, state) do
    if state.stop_on_error do
      send(state.caller, {:reduce_failed, error})
      {:stop, :normal, %__MODULE__{state | active_workers: new_active_workers}}
    else
      new_state = %__MODULE__{state | active_workers: new_active_workers}
      next_job(new_state, state.result)
    end
  end

  defp cleanup_completed_results(temp_completed_results, old_next_index, new_next_index) do
    if new_next_index > old_next_index do
      Enum.reduce(
        old_next_index..(new_next_index - 1),
        temp_completed_results,
        &Map.delete(&2, &1)
      )
    else
      temp_completed_results
    end
  end

  defp next_job(new_state, result) do
    if finished?(new_state) do
      send(new_state.caller, {:reduce_completed, result})
      {:stop, :normal, new_state}
    else
      final_state = fill_window(new_state)
      {:noreply, final_state}
    end
  end

  defp fill_window(state) do
    free_slots = state.window_size - map_size(state.active_workers)

    if free_slots > 0 do
      {jobs_to_run, remaining_jobs} = Enum.split(state.waiting_jobs, free_slots)

      new_active_workers =
        Enum.reduce(jobs_to_run, state.active_workers, fn {job, index}, acc ->
          {req_id, worker} = start_job(job)
          Map.put(acc, req_id, {worker, index})
        end)

      %__MODULE__{state | waiting_jobs: remaining_jobs, active_workers: new_active_workers}
    else
      state
    end
  end

  defp start_job(job) do
    worker = :poolboy.checkout(:worker_pool)
    req_id = :gen_server.send_request(worker, {:execute, job})
    {req_id, worker}
  end

  defp finished?(state) do
    state.waiting_jobs == [] and state.active_workers == %{}
  end

  defp process_completed_results(completed_results, next_index, current_result, reducer_func) do
    case Map.get(completed_results, next_index) do
      nil ->
        {current_result, next_index}

      value ->
        new_result = combine_result(current_result, value, reducer_func)
        process_completed_results(completed_results, next_index + 1, new_result, reducer_func)
    end
  end

  defp combine_result(nil, new_value, _) do
    new_value
  end

  defp combine_result(current, new_value, reducer_func) do
    reducer_func.(current, new_value)
  end
end
