defmodule MapReduce do
  @moduledoc """
  MapReduce is a library for distributed task execution.
  """

  alias MapReduce.Worker

  defdelegate create(), to: Worker
  defdelegate execute(worker, job), to: Worker
  defdelegate get_result(worker, job_id), to: Worker
  defdelegate reduce(worker, jobs, associative_func), to: Worker
end
