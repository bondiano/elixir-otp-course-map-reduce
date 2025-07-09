# MapReduce

## Домашка MapReduce

Процесс-исполнитель задачек

### API

1. Создавать задачу
  `create() -> task`

1. Запускать задачку
  `execute(task, fn -> any() end) -> task`

2. Получать результат задачки
  `get_result(task) -> result`

3. Получать статус задачки
  `get_status(task) -> status`

4. Функция, которая позволяет запустить несколько задачек и редьюснуть их ассоциативной функцией.
  `reduce([fn -> any() end], fn a, b -> any() end) -> result`


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

