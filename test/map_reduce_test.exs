defmodule MapReduce.WorkerTest do
  @moduledoc false

  use ExUnit.Case, async: true

  alias MapReduce

  describe "reduce/3" do
    test "reduces empty list" do
      jobs = []
      associative_func = fn a, b -> a + b end

      result = MapReduce.reduce(jobs, associative_func)
      assert result == {:ok, nil}
    end

    test "reduces single job" do
      jobs = [fn -> 42 end]
      associative_func = fn a, b -> a + b end

      result = MapReduce.reduce(jobs, associative_func)

      assert result == {:ok, 42}
    end

    test "reduces jobs with lists" do
      jobs = [
        fn -> [1, 2] end,
        fn -> [3, 4] end,
        fn -> [5, 6] end
      ]

      associative_func = fn a, b -> Enum.concat(a, b) end

      result = MapReduce.reduce(jobs, associative_func)

      assert result == {:ok, [1, 2, 3, 4, 5, 6]}
    end

    test "reduce with multiple jobs" do
      jobs = [fn -> 1 end, fn -> 2 * 2 end, fn -> 3 * 3 end, fn -> 4 * 4 end]
      result = MapReduce.reduce(jobs, fn a, b -> a + b end)
      assert result == {:ok, 30}
    end

    test "stop reduce on one error" do
      jobs = [fn -> 1 end, fn -> raise "Test error" end, fn -> 3 end]
      associative_func = fn a, b -> a + b end

      result = MapReduce.reduce(jobs, associative_func, stop_on_error: true)

      assert result == {:error, %RuntimeError{message: "Test error"}}
    end

    test "works with streams" do
      stream = Stream.map(1..5, fn x -> fn -> x * x end end)
      associative_func = fn a, b -> a + b end

      result = MapReduce.reduce(stream, associative_func)

      assert result == {:ok, 55}
    end

    test "works with infinite streams using window" do
      infinite_stream =
        Stream.iterate(1, &(&1 + 1))
        |> Stream.take(20)
        |> Stream.map(fn x -> fn -> x end end)

      associative_func = fn a, b -> a + b end

      result = MapReduce.reduce(infinite_stream, associative_func, window_size: 3)

      assert result == {:ok, 210}
    end

    test "works with ranges" do
      range_jobs = 1..4 |> Enum.map(fn x -> fn -> x * 2 end end)
      associative_func = fn a, b -> a + b end

      result = MapReduce.reduce(range_jobs, associative_func)

      assert result == {:ok, 20}
    end
  end
end
