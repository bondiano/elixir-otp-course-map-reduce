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
  @spec execute(t(), job(), Keyword.t()) :: job_id() | {:error, {:timeout, job_id()}}
  def execute(%__MODULE__{pid: pid}, job, opts \\ []) when is_function(job) do
    timeout = Keyword.get(opts, :timeout, 5000)
    job_id = Process.monitor(pid)
    caller_pid = self()

    send(pid, {:execute, job_id, job, caller_pid})

    receive do
      {:job_started, ^job_id} ->
        job_id
    after
      timeout ->
        Process.demonitor(job_id, [:flush])
        {:error, {:timeout, job_id}}
    end
  end

  @doc """
  Получает результат задачи по job_id
  """
  @spec get_result(t(), job_id(), Keyword.t()) :: any() | {:error, {:timeout, job_id()}}
  def get_result(%__MODULE__{pid: worker_pid}, job_id, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 5000)
    caller_pid = self()

    send(worker_pid, {:get_result, job_id, caller_pid})

    receive do
      {:result, ^job_id, result} -> {:ok, result}
      {:error, ^job_id, reason} -> {:error, reason}
      {:DOWN, ^job_id, :process, ^worker_pid, reason} -> {:error, {:died, job_id, reason}}
    after
      timeout ->
        Process.demonitor(job_id, [:flush])
        {:error, {:timeout, job_id}}
    end
  end

  @doc """
  Запускает несколько задач и применяет к результатам ассоциативную функцию
  """
  @spec reduce(t(), [job()], (any(), any() -> any())) :: {:ok, any()} | {:error, any()}
  def reduce(worker, jobs, associative_func)
      when is_list(jobs) and is_function(associative_func) do
    job_ids = Enum.map(jobs, &execute(worker, &1))
    await(worker, job_ids, associative_func)
  end

  defp await(worker, [job_id | rest], func) do
    case get_result(worker, job_id) do
      {:ok, result} -> await(worker, rest, func, result)
      {:error, reason} -> {:error, reason}
    end
  end

  defp await(_worker, [], _func) do
    {:ok, nil}
  end

  defp await(worker, [job_id | rest], func, acc) do
    case get_result(worker, job_id) do
      {:ok, result} -> await(worker, rest, func, func.(acc, result))
      {:error, reason} -> {:error, reason}
    end
  end

  defp await(_worker, [], _func, result) do
    {:ok, result}
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

        {:EXIT, pid, reason} ->
          handle_exit(state, pid, reason)
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

  defp handle_exit(state, pid, reason) do
    %{
      state
      | pending: Map.delete(state.pending, pid),
        results: Map.put(state.results, pid, {:error, {:died, pid, reason}})
    }
  end
end
