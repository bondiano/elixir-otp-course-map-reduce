defmodule MapReduce do
  @moduledoc """
  MapReduce is a library for distributed task execution.
  """

  alias MapReduce.{TaskManager, Task}

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
  Получает результат задачи
  """
  @spec get_result(Task.t()) :: {:ok, any()} | {:error, any()}
  defdelegate get_result(task), to: TaskManager

  @doc """
  Получает статус задачи
  """
  @spec get_status(Task.t()) :: :pending | :running | :completed | :failed
  defdelegate get_status(task), to: TaskManager

  @doc """
  Запускает несколько задач и применяет к результатам ассоциативную и коммутативную агрегатную функцию
  """
  @spec reduce([(-> any())], (any(), any() -> any())) :: {:ok, any()} | {:error, any()}
  defdelegate reduce(functions, aggregate_func), to: TaskManager
end
