defmodule DistributedTest do
  use ExUnit.Case, async: false

  @moduletag :distributed

  describe "Distributed MapReduce" do
    test "distributed reduce on single node works correctly" do
      jobs = 1..100 |> Enum.map(fn i -> {:erlang, :*, [i, 2]} end)

      {:ok, result} =
        MapReduce.distributed_reduce(
          jobs,
          fn acc, val -> acc + val end,
          window_size: 10,
          timeout: 10_000
        )

      expected = Enum.sum(1..100 |> Enum.map(&(&1 * 2)))
      assert result == expected
    end

    test "distributed reduce with empty jobs returns nil" do
      {:ok, result} =
        MapReduce.distributed_reduce(
          [],
          fn acc, val -> acc + val end,
          window_size: 10
        )

      assert result == nil
    end

    test "distributed reduce handles errors when stop_on_error is true" do
      defmodule TestError do
        def return_val(val), do: val
        def raise_error(_), do: raise("test error")
      end

      jobs = [
        {TestError, :return_val, [1]},
        {TestError, :raise_error, [nil]},
        {TestError, :return_val, [3]}
      ]

      {:error, _reason} =
        MapReduce.distributed_reduce(
          jobs,
          fn acc, val -> acc + val end,
          window_size: 2,
          stop_on_error: true
        )
    end

    test "distributed reduce continues on errors when stop_on_error is false" do
      defmodule TestErrorContinue do
        def return_val(val), do: val
        def raise_error(_), do: raise("test error")
      end

      jobs = [
        {TestErrorContinue, :return_val, [1]},
        {TestErrorContinue, :raise_error, [nil]},
        {TestErrorContinue, :return_val, [3]}
      ]

      {:ok, result} =
        MapReduce.distributed_reduce(
          jobs,
          fn acc, val -> acc + val end,
          window_size: 2,
          stop_on_error: false
        )

      # Должен получить как минимум 1, поскольку ошибочная задача игнорируется
      assert result >= 1
    end

    test "distributed reduce with large dataset" do
      jobs = 1..1000 |> Enum.map(fn i -> {:erlang, :+, [i, 0]} end)

      {:ok, result} =
        MapReduce.distributed_reduce(
          jobs,
          fn acc, val -> acc + val end,
          window_size: 50
        )

      expected = Enum.sum(1..1000)
      assert result == expected
    end

    test "distributed reduce performance comparison" do
      defmodule PerfTest do
        def slow_square(i) do
          Process.sleep(1)
          i * i
        end
      end

      jobs = 1..200 |> Enum.map(fn i -> {PerfTest, :slow_square, [i]} end)

      local_jobs = 1..200 |> Enum.map(fn i -> fn -> 
        Process.sleep(1)
        i * i
      end end)

      {local_time, {:ok, local_result}} =
        :timer.tc(fn ->
          MapReduce.reduce(local_jobs, fn acc, val -> acc + val end, window_size: 20)
        end)

      {distributed_time, {:ok, distributed_result}} =
        :timer.tc(fn ->
          MapReduce.distributed_reduce(jobs, fn acc, val -> acc + val end, window_size: 20)
        end)

      assert local_result == distributed_result

      assert local_time > 0
      assert distributed_time > 0

      IO.puts("\nПроизводительность:")
      IO.puts("Локальное: #{local_time / 1000} мс")
      IO.puts("Распределенное: #{distributed_time / 1000} мс")
    end
  end
end
