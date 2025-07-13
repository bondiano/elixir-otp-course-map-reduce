defmodule MapReduce.DistributedCoordinator do
  @moduledoc """
  Координатор для распределенного выполнения MapReduce задач на нескольких узлах
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
    :next_job_index,
    :ring,
    :node_managers,
    :timeout
  ]

  @type t :: %__MODULE__{
          caller: pid(),
          reducer_func: (any(), any() -> any()),
          window_size: pos_integer(),
          waiting_jobs: [{any(), non_neg_integer()}],
          active_workers: %{reference() => {node(), non_neg_integer()}},
          result: any(),
          stop_on_error: boolean(),
          completed_results: %{non_neg_integer() => any()},
          next_job_index: non_neg_integer(),
          ring: any(),
          node_managers: %{node() => pid()},
          timeout: pos_integer()
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
    timeout = Keyword.get(opts, :timeout, 60_000)

    nodes = [Node.self() | Node.list()]

    ring = HashRing.new() |> HashRing.add_nodes(nodes)

    node_managers = start_node_managers(nodes)

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
      next_job_index: 0,
      ring: ring,
      node_managers: node_managers,
      timeout: timeout
    }

    if Enum.empty?(state.waiting_jobs) do
      send(caller, {:reduce_completed, nil})
      {:ok, state, {:continue, :stop}}
    else
      {:ok, fill_window(state)}
    end
  end

  def handle_continue(:stop, state) do
    cleanup_node_managers(state.node_managers)
    {:stop, :normal, state}
  end

  def handle_info({:job_completed, req_id, result}, state) do
    case Map.pop(state.active_workers, req_id) do
      {{_node, job_index}, active_workers} ->
        task_finished(active_workers, result, job_index, state)

      {nil, _} ->
        {:noreply, state}
    end
  end

  def handle_info({:job_failed, req_id, reason}, state) do
    case Map.pop(state.active_workers, req_id) do
      {{_node, job_index}, active_workers} ->
        task_finished(active_workers, {:error, reason}, job_index, state)

      {nil, _} ->
        {:noreply, state}
    end
  end

  def handle_info({:DOWN, _ref, :process, _pid, reason}, state) do
    send(state.caller, {:reduce_failed, {:node_manager_died, reason}})
    {:stop, :normal, state}
  end

  defp start_node_managers(nodes) do
    Enum.reduce(nodes, %{}, fn node, acc ->
      case start_node_manager(node) do
        {:ok, pid} ->
          Process.monitor(pid)
          Map.put(acc, node, pid)

        {:error, _reason} ->
          acc
      end
    end)
  end

  defp start_node_manager(node) when node == node() do
    {:ok, spawn_link(fn -> node_manager_loop() end)}
  end

  defp start_node_manager(node) do
    case :rpc.call(node, __MODULE__, :start_remote_manager, [self()]) do
      {:ok, pid} when is_pid(pid) -> {:ok, pid}
      _ -> {:error, :failed_to_start_remote_manager}
    end
  end

  @spec start_remote_manager(pid()) :: {:ok, pid()}
  def start_remote_manager(coordinator_pid) do
    pid = spawn(fn -> remote_node_manager_loop(coordinator_pid) end)
    {:ok, pid}
  end

  defp cleanup_node_managers(node_managers) do
    Enum.each(node_managers, fn {node, pid} ->
      case node do
        n when n == node() ->
          if Process.alive?(pid) do
            Process.exit(pid, :normal)
          end

        remote_node ->
          :rpc.call(remote_node, Process, :exit, [pid, :normal])
      end
    end)
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
      cleanup_node_managers(state.node_managers)
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
      cleanup_node_managers(new_state.node_managers)
      send(new_state.caller, {:reduce_completed, result})
      {:stop, :normal, new_state}
    else
      final_state = fill_window(new_state)
      {:noreply, final_state}
    end
  end

  defp fill_window(state) do
    free_slots = state.window_size - map_size(state.active_workers)

    if free_slots > 0 and not Enum.empty?(state.waiting_jobs) do
      {jobs_to_run, remaining_jobs} = Enum.split(state.waiting_jobs, free_slots)

      new_active_workers =
        Enum.reduce(jobs_to_run, state.active_workers, fn {job, index}, acc ->
          job_key = :erlang.phash2(job)
          target_node = HashRing.key_to_node(state.ring, job_key)

          case Map.get(state.node_managers, target_node) do
            nil ->
              acc

            manager_pid ->
              req_id = make_ref()
              send(manager_pid, {:execute_job, req_id, job, self()})
              Map.put(acc, req_id, {target_node, index})
          end
        end)

      %__MODULE__{
        state
        | waiting_jobs: remaining_jobs,
          active_workers: new_active_workers
      }
    else
      state
    end
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

  defp node_manager_loop() do
    receive do
      {:execute_job, req_id, job, coordinator_pid} ->
        result = execute_job_locally(job)
        send(coordinator_pid, {:job_completed, req_id, result})
        node_manager_loop()
    end
  end

  defp remote_node_manager_loop(coordinator_pid) do
    receive do
      {:execute_job, req_id, job, _} ->
        result = execute_job_locally(job)
        send(coordinator_pid, {:job_completed, req_id, result})
        remote_node_manager_loop(coordinator_pid)
    end
  end

  defp execute_job_locally(job) do
    try do
      worker = :poolboy.checkout(:worker_pool)

      try do
        case GenServer.call(worker, {:execute, job}) do
          {:ok, result} -> {:ok, result}
          {:error, reason} -> {:error, reason}
        end
      after
        :poolboy.checkin(:worker_pool, worker)
      end
    catch
      kind, reason ->
        {:error, {kind, reason}}
    end
  end
end
