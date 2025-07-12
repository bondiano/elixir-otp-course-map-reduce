defmodule MapReduce do
  @moduledoc """
  MapReduce is a library for distributed task execution.
  """

  alias MapReduce.TaskManager

  @doc """
  Создает задачу
  """
  @spec create(Keyword.t()) :: {:ok, Task.t()} | {:error, any()}
  defdelegate create(opts \\ []), to: TaskManager

  @doc """
  Запускает задачу на исполнение
  """
  @spec execute(Task.t(), (-> any())) :: :ok
  defdelegate execute(task, func), to: TaskManager

  @doc """
  Запускает несколько задач и применяет к результатам ассоциативную и коммутативную агрегатную функцию
  """
  @spec reduce([(-> any())], (any(), any() -> any())) :: {:ok, any()} | {:error, any()}
  defdelegate reduce(functions, aggregate_func), to: TaskManager
end
