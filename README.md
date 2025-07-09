# MapReduce

## Домашка MapReduce

Процесс-исполнитель задачек

### API

1. Создавать исполнителя и прилинковывать его к вызывающему процессу
  `create() -> worker`

2. Запускать задачку
  `execute(worker, job) -> job_id`

3. Получать результат задачки по `job_id`
  `get_result(worker, job_id) -> result`

4. Функция, которая позволяет запустить несколько задачек и редьюснуть их ассоциативной функцией.
  `reduce(worker, jobs, associative_func)`

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `map_reduce` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:map_reduce, "~> 0.1.0"}
  ]
end
```

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at <https://hexdocs.pm/map_reduce>.

