defmodule Examples.BasicUsage do
  alias MapReduce.Worker

  def run_basic_examples do
    IO.puts("🗺️  === Basic MapReduce Examples ===  🗺️\n")

    worker = Worker.create()
    IO.puts("🛠️   1. Created worker: #{inspect(worker.pid)}")

    simple_job = fn ->
      IO.puts("    ⚙️  Executing simple job...")
      Process.sleep(100)
      42
    end

    job_id = Worker.execute(worker, simple_job)
    IO.puts("🚀  2. Started job with ID: #{inspect(job_id)}")

    {:ok, result} = Worker.get_result(worker, job_id)
    IO.puts("✅  3. Got result: #{inspect(result)}\n")

    IO.puts("⚡  4. Running multiple jobs in parallel...")
    jobs = [
      fn ->
        Process.sleep(Enum.random(50..150))
        1
      end,
      fn ->
        Process.sleep(Enum.random(50..150))
        2
      end,
      fn ->
        Process.sleep(Enum.random(50..150))
        3
      end
    ]

    start_time = System.monotonic_time(:millisecond)
    sum = Worker.reduce(worker, jobs, fn a, b -> a + b end)
    end_time = System.monotonic_time(:millisecond)

    IO.puts("    ➕ Sum of parallel jobs: #{sum}")
    IO.puts("    ⏱️  Time taken: #{end_time - start_time}ms\n")

    # MapReduce для подсчета слов
    IO.puts("🔤  5. Word count MapReduce example:")
    word_count_example(worker)
  end

  defp word_count_example(worker) do
    texts = [
      "hello world hello",
      "world of elixir",
      "elixir is great",
      "hello elixir world",
      "hello elixir world",
      "hello elixir world",
    ]

    map_jobs = Enum.map(texts, fn text ->
      fn ->
        text
        |> String.split()
        |> Enum.frequencies()
      end
    end)

    word_counts = Worker.reduce(worker, map_jobs, fn freq1, freq2 ->
      Map.merge(freq1, freq2, fn _key, v1, v2 -> v1 + v2 end)
    end)

    IO.puts("     📊 Word frequencies:")
    Enum.each(word_counts, fn {word, count} ->
      IO.puts("     📝 #{word}: #{count}")
    end)

    IO.puts("")
  end
end

Examples.BasicUsage.run_basic_examples()
