defmodule SimpleTest do
  def run do
    IO.puts("=== Простой тест ===")
    
    # Простые задачи
    jobs = 1..5 |> Enum.map(fn i -> {:erlang, :*, [i, 2]} end)
    
    result = MapReduce.distributed_reduce(
      jobs,
      fn acc, val -> acc + val end,
      window_size: 2,
      timeout: 10_000
    )
    
    case result do
      {:ok, sum} ->
        expected = Enum.sum(1..5 |> Enum.map(&(&1 * 2)))
        IO.puts("Результат: #{sum}, ожидается: #{expected}")
        IO.puts("Тест #{if sum == expected, do: "ПРОЙДЕН", else: "ПРОВАЛЕН"}")
      
      {:error, reason} ->
        IO.puts("ОШИБКА: #{inspect(reason)}")
    end
  end
end

SimpleTest.run()