defmodule MapReduce.TaskManager do
  @moduledoc """
  Менеджер для распределения задач по процессам
  Window на старт + HashRing для распределения
  """

  use GenServer

  defstruct [:hash_ring, :window_size]

  @default_window_size 100

  @me __MODULE__

  def start_link(opts \\ []) do
    GenServer.start_link(@me, opts, name: @me)
  end

  def process_jobs(jobs, reducer_func) when is_list(jobs) and is_function(reducer_func, 2) do
    GenServer.call(@me, {:process_jobs, jobs, reducer_func})
  end

  @impl true
  def init(opts) do
    window_size = Keyword.get(opts, :window_size, @default_window_size)

    ring =
      HashRing.new()
      # виртуальные ноды для распределения
      |> HashRing.add_nodes(["worker_1", "worker_2", "worker_3", "worker_4"])

    {:ok, %__MODULE__{hash_ring: ring, window_size: window_size}}
  end

  @impl true
  def handle_call({:process_jobs, jobs, reducer_func}, from, state) do
    windows = create_windows(jobs, state.window_size)

    spawn_link(fn ->
      result = process_windows_parallel(windows, reducer_func, state.hash_ring)
      GenServer.reply(from, result)
    end)

    {:noreply, state}
  end

  defp create_windows(jobs, window_size) do
    Enum.chunk_every(jobs, window_size)
  end

  defp process_windows_parallel(windows, reducer_func, hash_ring) do
    parent = self()

    window_tasks =
      Enum.with_index(windows)
      |> Enum.map(fn {window_jobs, index} ->
        spawn_link(fn ->
          results = process_window(window_jobs, hash_ring)
          send(parent, {:window_completed, index, results})
        end)
      end)

    window_results = collect_window_results(length(windows), %{})

    all_results =
      window_results
      |> Map.values()
      |> List.flatten()
      |> Enum.filter(fn
        {:ok, _} -> true
        _ -> false
      end)
      |> Enum.map(fn {:ok, result} -> result end)

    apply_reducer(all_results, reducer_func)
  end

  defp process_window(jobs, hash_ring) do
    Enum.map(jobs, fn job ->
      execute_job_with_routing(job, hash_ring)
    end)
  end

  defp collect_window_results(0, acc), do: acc

  defp collect_window_results(remaining, acc) do
    receive do
      {:window_completed, index, results} ->
        new_acc = Map.put(acc, index, results)
        collect_window_results(remaining - 1, new_acc)
    after
      30_000 -> acc
    end
  end

  defp execute_job_with_routing(job, hash_ring) do
    :poolboy.transaction(:map_reduce_pool, fn worker ->
      GenServer.call(worker, {:execute, job})
    end)
  end

  defp apply_reducer([], _reducer_func), do: {:ok, nil}
  defp apply_reducer([single], _reducer_func), do: {:ok, single}

  defp apply_reducer([first | rest], reducer_func) do
    try do
      result = Enum.reduce(rest, first, reducer_func)
      {:ok, result}
    rescue
      error -> {:error, error}
    end
  end
end
