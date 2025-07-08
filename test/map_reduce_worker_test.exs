defmodule MapReduce.WorkerTest do
  @moduledoc false

  use ExUnit.Case, async: true

  alias MapReduce.Worker

  describe "create/0" do
    test "creates a worker process" do
      worker = Worker.create()

      assert %Worker{pid: pid, monitor_ref: ref} = worker
      assert is_pid(pid)
      assert is_reference(ref)
      assert Process.alive?(pid)
    end

    test "executes multiple jobs with different job_ids" do
      worker = Worker.create()
      job1 = fn -> 1 + 1 end
      job2 = fn -> 2 + 2 end

      job_id1 = Worker.execute(worker, job1)
      job_id2 = Worker.execute(worker, job2)

      assert job_id1 != job_id2
    end
  end

  describe "get_result/2" do
    test "gets result of completed job" do
      worker = Worker.create()
      job = fn -> 42 end

      job_id = Worker.execute(worker, job)
      result = Worker.get_result(worker, job_id)

      assert result == 42
    end

    test "handles job errors" do
      worker = Worker.create()
      job = fn -> raise "Test error" end

      job_id = Worker.execute(worker, job)
      result = Worker.get_result(worker, job_id)

      assert {:error, %RuntimeError{message: "Test error"}} = result
    end

    test "multiple callers can get same result" do
      worker = Worker.create()

      job = fn ->
        Process.sleep(50)
        :shared_result
      end

      job_id = Worker.execute(worker, job)

      parent = self()

      for i <- 1..3 do
        spawn_link(fn ->
          result = Worker.get_result(worker, job_id)
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

      assert Enum.all?(results, &(&1 == :shared_result))
    end
  end

  describe "reduce/3" do
    test "reduces empty list" do
      worker = Worker.create()
      jobs = []
      associative_func = fn a, b -> a + b end

      result = Worker.reduce(worker, jobs, associative_func)

      assert result == nil
    end

    test "reduces single job" do
      worker = Worker.create()
      jobs = [fn -> 42 end]
      associative_func = fn a, b -> a + b end

      result = Worker.reduce(worker, jobs, associative_func)

      assert result == 42
    end

    test "reduces jobs with lists" do
      worker = Worker.create()

      jobs = [
        fn -> [1, 2] end,
        fn -> [3, 4] end,
        fn -> [5, 6] end
      ]

      associative_func = fn a, b -> Enum.concat(b, a) end

      result = Worker.reduce(worker, jobs, associative_func)

      assert result == [1, 2, 3, 4, 5, 6]
    end
  end

  describe "concurrent operations" do
    test "handles concurrent job execution" do
      worker = Worker.create()
      parent = self()

      for i <- 1..5 do
        spawn_link(fn ->
          job = fn ->
            Process.sleep(Enum.random(10..50))
            i * 10
          end

          job_id = Worker.execute(worker, job)
          result = Worker.get_result(worker, job_id)
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

      assert Enum.all?(results, fn {i, result} -> result == i * 10 end)
    end
  end

  describe "error handling" do
    test "handles job that throws" do
      worker = Worker.create()
      job = fn -> throw(:test_throw) end

      job_id = Worker.execute(worker, job)
      result = Worker.get_result(worker, job_id)

      assert {:error, {:throw, :test_throw}} = result
    end

    test "handles job that exits" do
      worker = Worker.create()
      job = fn -> exit(:test_exit) end

      job_id = Worker.execute(worker, job)
      result = Worker.get_result(worker, job_id)

      assert {:error, {:exit, :test_exit}} = result
    end
  end
end
