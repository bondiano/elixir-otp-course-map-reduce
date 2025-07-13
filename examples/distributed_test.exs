defmodule Examples.Distributed do
  @moduledoc """
  Тест распределенного MapReduce на нескольких узлах с использованием local_cluster
  """

  def run do
    IO.puts("=== Запуск распределенного теста MapReduce ===")

    # Запускаем local_cluster
    :ok = LocalCluster.start()

    {:ok, cluster} =
      LocalCluster.start_link(3,
        applications: [:map_reduce],
        prefix: "mapreduce_test_#{:rand.uniform(10000)}"
      )

    {:ok, nodes} = LocalCluster.nodes(cluster)
    IO.puts("Запущены узлы: #{inspect(nodes)}")

    # Тестируем распределенное выполнение
    test_distributed_mapreduce(nodes)

    # Тестируем отказоустойчивость
    test_fault_tolerance(nodes, cluster)

    # Останавливаем кластер
    LocalCluster.stop(cluster)
    IO.puts("=== Тест завершен ===")
  end

  defp test_distributed_mapreduce(nodes) do
    IO.puts("\n--- Тест базовой функциональности ---")

    # Создаем большую задачу для распределения по узлам (используем MFA для сериализации)
    jobs = 1..1000 |> Enum.map(fn i -> {:erlang, :*, [i, i]} end)

    start_time = System.monotonic_time()

    # Выполняем распределенное MapReduce
    result = MapReduce.distributed_reduce(
      jobs,
      fn acc, val -> acc + val end,
      window_size: 50,
      timeout: 30_000
    )

    end_time = System.monotonic_time()
    duration = System.convert_time_unit(end_time - start_time, :native, :millisecond)

    case result do
      {:ok, sum} ->
        expected_sum = Enum.sum(1..1000 |> Enum.map(&(&1 * &1)))
        IO.puts("Результат: #{sum}")
        IO.puts("Ожидается: #{expected_sum}")
        IO.puts("Время выполнения: #{duration} мс")
        IO.puts("Тест #{if sum == expected_sum, do: "ПРОЙДЕН", else: "ПРОВАЛЕН"}")

      {:error, reason} ->
        IO.puts("ОШИБКА: #{inspect(reason)}")
    end

    # Проверяем статистику пулов на всех узлах
    IO.puts("\nСтатистика пулов воркеров:")
    for node <- [Node.self() | nodes] do
      stats = :rpc.call(node, MapReduce, :pool_stats, [])
      IO.puts("#{node}: #{inspect(stats)}")
    end
  end

  defp test_fault_tolerance(nodes, cluster) do
    IO.puts("\n--- Тест отказоустойчивости ---")

    # Создаем долгие задачи (используем MFA для сериализации)
    defmodule SlowTask do
      def slow_multiply(i) do
        Process.sleep(100)
        i * 2
      end
    end

    jobs = 1..100 |> Enum.map(fn i -> {SlowTask, :slow_multiply, [i]} end)

    # Запускаем выполнение в отдельном процессе
    parent = self()

    test_pid = spawn(fn ->
      result = MapReduce.distributed_reduce(
        jobs,
        fn acc, val -> acc + val end,
        window_size: 20,
        timeout: 60_000
      )
      send(parent, {:test_result, result})
    end)

    Process.sleep(2000)
    [node_to_stop | _] = nodes
    IO.puts("Останавливаем узел: #{node_to_stop}")
    LocalCluster.stop(cluster, node_to_stop)

    receive do
      {:test_result, result} ->
        case result do
          {:ok, sum} ->
            expected_sum = Enum.sum(1..100 |> Enum.map(&(&1 * 2)))
            IO.puts("Результат с отказом узла: #{sum}")
            IO.puts("Ожидается: #{expected_sum}")
            IO.puts("Тест отказоустойчивости #{if sum == expected_sum, do: "ПРОЙДЕН", else: "ПРОВАЛЕН"}")

          {:error, reason} ->
            IO.puts("ОШИБКА при тесте отказоустойчивости: #{inspect(reason)}")
        end
    after
      65_000 ->
        Process.exit(test_pid, :kill)
        IO.puts("ТАЙМАУТ при тесте отказоустойчивости")
    end
  end

  def benchmark_distributed_vs_local do
    IO.puts("\n=== Бенчмарк: Распределенное vs Локальное выполнение ===")

    # Запускаем local_cluster
    :ok = LocalCluster.start()

    {:ok, cluster} =
      LocalCluster.start_link(3,
        applications: [:map_reduce],
        prefix: "benchmark_#{:rand.uniform(10000)}"
      )

    {:ok, _nodes} = LocalCluster.nodes(cluster)

    defmodule ComplexTask do
      def heavy_computation(i) do
        Enum.reduce(1..1000, 0, fn j, acc -> acc + i * j end)
      end
    end

    jobs = 1..500 |> Enum.map(fn i -> {ComplexTask, :heavy_computation, [i]} end)

    IO.puts("Тестирование локального выполнения...")
    {local_time, {:ok, local_result}} = :timer.tc(fn ->
      MapReduce.reduce(jobs, fn acc, val -> acc + val end, window_size: 50)
    end)

    IO.puts("Тестирование распределенного выполнения...")
    {distributed_time, {:ok, distributed_result}} = :timer.tc(fn ->
      MapReduce.distributed_reduce(jobs, fn acc, val -> acc + val end, window_size: 50)
    end)

    IO.puts("\nРезультаты бенчмарка:")
    IO.puts("Локальное выполнение: #{local_time / 1000} мс, результат: #{local_result}")
    IO.puts("Распределенное выполнение: #{distributed_time / 1000} мс, результат: #{distributed_result}")
    IO.puts("Ускорение: #{Float.round(local_time / distributed_time, 2)}x")
    IO.puts("Результаты совпадают: #{local_result == distributed_result}")

    LocalCluster.stop(cluster)
  end
end

Examples.Distributed.run()

Examples.Distributed.benchmark_distributed_vs_local()
