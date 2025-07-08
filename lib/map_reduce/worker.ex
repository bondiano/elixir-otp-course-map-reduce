defmodule MapReduce.Worker do
  @moduledoc """
  Процесс-исполнитель задач с поддержкой MapReduce операций
  """

  defstruct [:pid, :monitor_ref]

  @type t :: %__MODULE__{
          pid: pid(),
          monitor_ref: reference()
        }
  @type job_id :: reference()
  @type job :: (-> any())

  @doc """
  Создает новый процесс-исполнитель и связывает его с вызывающим процессом
  """
  @spec create() :: t()
  def create() do
    {pid, monitor_ref} = spawn_monitor(&init/0)
    %__MODULE__{pid: pid, monitor_ref: monitor_ref}
  end

  @doc """
  Запускает задачу на исполнение
  """
  @spec execute(t(), job()) :: job_id()
  def execute(%__MODULE__{pid: pid}, job) when is_function(job) do
    job_id = make_ref()
    caller_pid = self()

    send(pid, {:execute, job_id, job, caller_pid})

    receive do
      {:job_started, ^job_id} ->
        job_id
    after
      5000 ->
        raise "Worker did not acknowledge job started"
    end
  end

  @doc """
  Получает результат задачи по job_id
  """
  @spec get_result(t(), job_id()) :: any()
  def get_result(%__MODULE__{pid: worker_pid}, job_id) do
    caller_pid = self()

    send(worker_pid, {:get_result, job_id, caller_pid})

    receive do
      {:result, ^job_id, result} -> result
      {:error, ^job_id, reason} -> {:error, reason}
    end
  end

  @doc """
  Запускает несколько задач и применяет к результатам ассоциативную функцию
  """
  @spec reduce(t(), [job()], (any(), any() -> any())) :: any()
  def reduce(worker, jobs, associative_func)
      when is_list(jobs) and is_function(associative_func) do
    job_ids = Enum.map(jobs, &execute(worker, &1))

    results = Enum.map(job_ids, &get_result(worker, &1))

    case results do
      [] -> nil
      [single_result] -> single_result
      [first | rest] -> Enum.reduce(rest, first, associative_func)
    end
  end

  defp init() do
    Process.flag(:trap_exit, true)

    loop(%{results: %{}, pending: %{}})
  end

  defp loop(state) do
    state =
      receive do
        {:execute, job_id, job, caller_pid} ->
          handle_execute(job_id, job, caller_pid, state)

        {:get_result, job_id, caller_pid} ->
          handle_get_result(job_id, caller_pid, state)

        {:job_completed, job_id, result} ->
          handle_job_completed(job_id, result, state)

        {:job_failed, job_id, error} ->
          handle_job_failed(job_id, error, state)

        _ ->
          state
      end

    loop(state)
  end

  defp handle_execute(job_id, job, caller_pid, state) do
    send(caller_pid, {:job_started, job_id})

    worker_pid = self()

    spawn_link(fn ->
      try do
        result = job.()
        send(worker_pid, {:job_completed, job_id, result})
      rescue
        error ->
          send(worker_pid, {:job_failed, job_id, error})
      catch
        :exit, reason ->
          send(worker_pid, {:job_failed, job_id, {:exit, reason}})

        :throw, reason ->
          send(worker_pid, {:job_failed, job_id, {:throw, reason}})
      end
    end)

    %{state | pending: Map.put(state.pending, job_id, [worker_pid])}
  end

  defp handle_get_result(job_id, caller_pid, state) do
    case Map.get(state.results, job_id) do
      nil ->
        pending_callers = Map.get(state.pending, job_id, [])
        new_pending = Map.put(state.pending, job_id, [caller_pid | pending_callers])
        %{state | pending: new_pending}

      {:ok, result} ->
        send(caller_pid, {:result, job_id, result})
        state

      {:error, error} ->
        send(caller_pid, {:error, job_id, error})
        state
    end
  end

  defp handle_job_completed(job_id, result, state) do
    new_results = Map.put(state.results, job_id, {:ok, result})

    pending_callers = Map.get(state.pending, job_id, [])

    Enum.each(pending_callers, fn caller_pid ->
      send(caller_pid, {:result, job_id, result})
    end)

    new_pending = Map.delete(state.pending, job_id)

    %{state | results: new_results, pending: new_pending}
  end

  defp handle_job_failed(job_id, error, state) do
    new_results = Map.put(state.results, job_id, {:error, error})

    pending_callers = Map.get(state.pending, job_id, [])

    Enum.each(pending_callers, fn caller_pid ->
      send(caller_pid, {:error, job_id, error})
    end)

    new_pending = Map.delete(state.pending, job_id)

    %{state | results: new_results, pending: new_pending}
  end
end
