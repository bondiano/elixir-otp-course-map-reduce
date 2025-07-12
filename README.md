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
