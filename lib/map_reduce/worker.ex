defmodule MapReduce.Worker do
  @moduledoc """
  Воркер процесс для выполнения задач
  """

  use GenServer

  def start_link(_) do
    GenServer.start_link(__MODULE__, :ok)
  end

  @impl true
  def init(:ok) do
    {:ok, %{}}
  end

  @impl true
  def handle_call({:execute, job}, _from, state) when is_function(job, 0) do
    try do
      result = job.()
      {:reply, {:ok, result}, state}
    rescue
      error -> {:reply, {:error, error}, state}
    catch
      :exit, reason -> {:reply, {:error, {:exit, reason}}, state}
      :throw, value -> {:reply, {:error, {:throw, value}}, state}
    end
  end
end
