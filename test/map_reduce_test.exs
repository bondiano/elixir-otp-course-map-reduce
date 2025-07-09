defmodule MapReduce.WorkerTest do
  @moduledoc false

  use ExUnit.Case, async: true

  alias MapReduce
  alias MapReduce.Worker

  describe "create/0" do
    test "creates a worker process" do
      worker = MapReduce.create()

      assert %Worker{pid: pid, monitor_ref: ref} = worker
      assert is_pid(pid)
      assert is_reference(ref)
      assert Process.alive?(pid)
    end

    test "executes multiple jobs with different job_ids" do
      worker = MapReduce.create()
      job1 = fn -> 1 + 1 end
      job2 = fn -> 2 + 2 end

      job_id1 = MapReduce.execute(worker, job1)
      job_id2 = MapReduce.execute(worker, job2)

      assert job_id1 != job_id2
    end
  end

  describe "get_result/2" do
    test "gets result of completed job" do
      worker = MapReduce.create()
      job = fn -> 42 end

      job_id = MapReduce.execute(worker, job)
      result = MapReduce.get_result(worker, job_id)

      assert result == {:ok, 42}
    end

    test "handles job errors" do
      worker = MapReduce.create()
      job = fn -> raise "Test error" end

      job_id = MapReduce.execute(worker, job)
      result = MapReduce.get_result(worker, job_id)

      assert {:error, %RuntimeError{message: "Test error"}} = result
    end

    test "multiple callers can get same result" do
      worker = MapReduce.create()

      job = fn ->
        Process.sleep(50)
        :shared_result
      end

      job_id = MapReduce.execute(worker, job)

      parent = self()

      for i <- 1..3 do
        spawn_link(fn ->
          result = MapReduce.get_result(worker, job_id)
          send(parent, {:result, i, result})
        end)
      end

      results =
        for _i <- 1..3 do
          receive do
            {:result, _i, result} -> result
          after
            1000 -> :timeout
          end
        end

      assert Enum.all?(results, &(&1 == {:ok, :shared_result}))
    end
  end

  describe "reduce/3" do
    test "reduces empty list" do
      worker = MapReduce.create()
      jobs = []
      associative_func = fn a, b -> a + b end

      result = MapReduce.reduce(worker, jobs, associative_func)

      assert result == {:ok, nil}
    end

    test "reduces single job" do
      worker = MapReduce.create()
      jobs = [fn -> 42 end]
      associative_func = fn a, b -> a + b end

      result = MapReduce.reduce(worker, jobs, associative_func)

      assert result == {:ok, 42}
    end

    test "reduces jobs with lists" do
      worker = MapReduce.create()

      jobs = [
        fn -> [1, 2] end,
        fn -> [3, 4] end,
        fn -> [5, 6] end
      ]

      associative_func = fn a, b -> Enum.concat(a, b) end

      result = MapReduce.reduce(worker, jobs, associative_func)

      assert result == {:ok, [1, 2, 3, 4, 5, 6]}
    end

    test "reduce with multiple jobs" do
      worker = MapReduce.create()
      jobs = [fn -> 1 end, fn -> 2 * 2 end, fn -> 3 * 3 end, fn -> 4 * 4 end]
      result = MapReduce.reduce(worker, jobs, fn a, b -> a + b end)
      assert result == {:ok, 30}
    end

    test "stop reduce on one error" do
      worker = MapReduce.create()
      jobs = [fn -> 1 end, fn -> raise "Test error" end, fn -> 3 end]
      associative_func = fn a, b -> a + b end

      result = MapReduce.reduce(worker, jobs, associative_func)

      assert result == {:error, %RuntimeError{message: "Test error"}}
    end
  end

  describe "concurrent operations" do
    test "handles concurrent job execution" do
      worker = MapReduce.create()
      parent = self()

      for i <- 1..5 do
        spawn_link(fn ->
          job = fn ->
            Process.sleep(Enum.random(10..50))
            i * 10
          end

          job_id = MapReduce.execute(worker, job)
          result = MapReduce.get_result(worker, job_id)
          send(parent, {:completed, i, result})
        end)
      end

      results =
        for _i <- 1..5 do
          receive do
            {:completed, i, result} -> {i, result}
          after
            1000 -> {:timeout, :timeout}
          end
        end

      assert Enum.all?(results, fn {i, result} -> result == {:ok, i * 10} end)
    end
  end

  describe "error handling" do
    test "handles job that throws" do
      worker = MapReduce.create()
      job = fn -> throw(:test_throw) end

      job_id = MapReduce.execute(worker, job)
      result = MapReduce.get_result(worker, job_id)

      assert {:error, {:throw, :test_throw}} = result
    end

    test "handles job that exits" do
      worker = MapReduce.create()
      job = fn -> exit(:test_exit) end

      job_id = MapReduce.execute(worker, job)
      result = MapReduce.get_result(worker, job_id)

      assert {:error, {:exit, :test_exit}} = result
    end
  end
end
