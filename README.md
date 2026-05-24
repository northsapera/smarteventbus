![Python Version](https://img.shields.io/badge/python-3.11%2B-darkblue?style=flat-square) ![License](https://img.shields.io/badge/license-Apache%202.0-darkgreen?style=flat-square) ![Status](https://img.shields.io/badge/status-beta-yellow?style=flat-square)
# Smart Event Bus
## Краткое описание

Эта шина событий создана для **безопасного** управления *событиями* с ориентированием на **многопоточную** среду. Работает по принципу *издатель-шина-подписчик*.
### Особенности шины

Особенностью шины является **потокобезопасность**, поддержка **асинхронности** и **сложная логика работы с событиями** и **очередью событий**. Поддерживает возможность расстановки по **приоритету**, **контроля** и **учета** событий в очереди, **замены** и **отключения** событий, синхронизации потоков через **работу с наполнением** очереди, публикации событий из **любого потока** и проброс **асинхронных** хэндлеров.

### Почему Smart?

Философия созданной шины заключается в изменении подхода к системе событий - вместо попытки как можно быстрее пробросить их сквозь, эта шина берет роль **диспетчера**, которому можно задать логику и условия взаимодействия событий между собой и окружением. Это позволяет **гибко и масштабируемо** использовать шину в разных условиях - от медленных и ответственных процессов, где нельзя позволить себе потерять данные из-за ошибки, до динамической визуальной отрисовки, где нужна скорость, но есть возможность безболезненно "опустить" часть информации. **Низкая связность**, **приоритетная очередь**, использование **логики уникальности**, работа с обособленными событиями, строгая типизация через **контейнеры**, **потокобезопасность** и поддержка **асинхронности** делает эту шину универсально применимой в **большинстве** ситуаций. Подробнее см. [*'Логика работы'*](#логика-работы) и [*'Примеры использования'*](#примеры-использования).

### Логика работы

Ядром шины является **приоритетная потокобезопасная очередь** с **учетом** вложенных элементов. При публикации событие упаковывается в **строго типизированный контейнер**, основанный на *Pydantic-модели*, и кладется в очередь согласно указанной **логике уникальности**. В данный момент поддерживается три типа:

| Тип (UniqType) | Описание логики | Применение |
| :---: | :--- | :--- |
| **NONE** | Событие просто добавляется в очередь. | Обычные уведомления, где важен каждый факт события. |
| **WAIT** | Ждет, если в ней уже находится *n* идентичных событий. | Синхронизация потоков по самому медленному участнику. |
| **REPLACE** | Заменяет *n-ное* идентичное событие в очереди (с начала). | Оптимизация: обновление прогрессбаров, когда важен только актуальный статус. |

Идентичность событий оценивается по их *типу* (имени класса), *имени* и *метаданным*. События с идентичными типом, именем и метаданными считаются одинаковыми **вне зависимости от наполнения**.

Публикация событий доступна из **любого потока**, активация хэндлеров (подписчиков) происходит либо в **общем пуле потоков**, либо в **выделенном потоке-воркере**. В качестве подписчиков выступают **функции** или "зарегистрированные" **хэндлеры** (объект *Handler*), **защищающие и дополняющие** упакованные функции. При этом и **функции**, и **хэндлеры** могут быть как **синхронные**, так и **асинхронные**. В данный момент поддерживается три типа подписки:

| Тип (SubscriptionType) | Критерий активации | Точность |
| :---: | :--- | :---: |
| **NAME** | Совпадение *имени* события. | Низкая (широкий охват) |
| **ID** | Совпадение *типа* (имени класса), *имени* и *метаданных*. | Средняя |
| **NUMBER** | Точное совпадение *порядкового номера*. | Максимальная (адресная связь) |

При регистрации подписчиков через **хэндлеры** существует возможность расширенной настройки взаимодействия с *циклом обработки событий*:
- Строгий порядок выполнения (**strict_order**):
1. *True*: цикл обработки событий ждет конца выполнения хэндлера прежде, чем перейти к следующему подписчику или событию;
2. *False*: цикл обработки событий не ждет конца выполнения хэндлера, сразу переходит к следующему подписчику или событию;
- Поток выполнения (**ThreadType**):
1. *POOL*: хэндлер выполняется в общем пуле-воркере;
2. *DEDICATED*: хэндлер выполняется в выделенном потоке.

## Планы развития

Текущая работа заключается в:
- Добавлении поддержки проброса в целевой поток
- Добавлении поддержки мультипроцессинга
- Добавлении неблокирующих асинхронных методов публикации событий
- Расширении логики уникальности
- Обильном покрытии тестами
- Создании подробной документации

## Quick Start
### Установка через pip

Как обычную библиотеку (без скачивания исходников):

```bash
pip install git+https://github.com/northsapera/smarteventbus.git
```

Для разработки (если вы клонировали репозиторий):
```bash
git clone https://github.com/northsapera/smarteventbus.git
cd smarteventbus

# Виртуальное окружение
python -m venv .venv
# Для Windows:
.venv\Scripts\activate
# Для Linux/macOS:
source .venv/bin/activate

# Установка библиотеки с динамической связью
pip install -e .
```
## Примеры использования
### Запуск и использование [с явным определением](./examples/readme_short_example.py):

```python
from smarteventbus import Event, Handler, bus

bus.start()


def hello(greet, target):
    print(f"{greet}, {target}!")


hello_handler = Handler(func=hello, default_kwargs={"greet": "Hello"})
hello_event = Event(name="greetings", kwargs={"target": "World"})

bus.subscribe(hello_event, hello_handler)
bus.publish(hello_event)
# Hello, World!

bus.stop()

```

### Упрощенный синтаксис с [декораторами](./examples/readme_decorators_example.py):

```python
from smarteventbus import Event, EventBus, register, subscribe_to

bus = EventBus()

bus.start()

# Define an event with dynamic context
example_event = Event(
    name="Init event",
    kwargs={"txt": "This line will appear after the event is published!"},
)


@subscribe_to(bus, example_event)
@register(default_kwargs={"txt": "This line will be in the output!"})
def func_print(txt: str, printing: bool = True) -> None:
    """This docstring is visible!"""
    if printing:
        print(txt)


# 1. Direct call: invalid argument won't crash the code since it is registered as a Handler!
func_print(
    invalid_argument="This line won't crash the code thanks to the Handler!",
)
# Output: This line will be in the output!

# 2. Event-driven execution: triggering the function via the event bus
bus.publish(example_event)
# Output: This line will appear after the event is published!

# Gracefully stops the bus, waiting for all task_done signals (timeout: 10s)
bus.stop()

```

### [Развернутый пример с пояснениями](./examples/readme_detailed_example.py):

<details>
<summary>Подробно</summary>

```python
import threading

from smarteventbus import BusNetwork, Event, Handler, TyEv, UniqType, bus, debug_mode
from smarteventbus import SubscribeType as SubType

# Enable debug mode
debug_mode.set()

# Start the event bus
bus.start()


# Create classes connected to the bus (needed if publishing events from within the class)
class Logic(BusNetwork):
    def __init__(self):
        self.bus = super().bus

    def calc(self, num1: int, num2: int, end_event: Event):
        result = num1 + num2

        # Create an event (event name, payload data, priority, uniqueness logic)
        got_result = Event(
            name="calc complete",
            kwargs={"result": result},
            priority=10,
            uniq_type=UniqType.WAIT,
        )

        # Publish the result
        self.bus.publish(got_result)
        self.bus.publish(end_event)

    def calc_to_thread(self, num1: int, num2: int, end_event: Event):
        """Run calculations in a separate thread"""
        self.calc_thread = threading.Thread(
            target=self.calc,
            kwargs={"num1": num1, "num2": num2, "end_event": end_event},
            daemon=True,
        )

        self.calc_thread.start()


logic = Logic()

""" - Inside the main thread: - """


def start_print(**kwargs):
    print("Start!")


def print_result(result: int, **kwargs):
    print(result)


def print_txt(**kwargs):
    print(kwargs)
    print("No problems.")


def end_calc():
    print("Calculations complete!")


# Create handlers (subscribers)
calc_handler = Handler(func=logic.calc_to_thread, default_kwargs={"num1": 5})
end_handler = Handler(func=end_calc)

# Subscribe to events
# TyEv.START is a built-in event type with default values; its parameters can be customized just like a regular event
bus.subscribe(
    TyEv.START, [start_print, calc_handler]
)  # Providing multiple subscribers in a list guarantees their execution order

# If a subscriber is a standard function, it's recommended to include **kwargs in its arguments.
# The bus passes events "as-is" with all payload and internal parameters (when debug_mode is active).
bus.subscribe("calc complete", print_result)

# Subscription by ID does not accept strings
bus.subscribe(Event(name="calc complete"), print_txt, SubType.ID)

# Subscription by NUMBER accepts a specific event object or index/number
end_event = TyEv.END()
bus.subscribe(end_event, end_handler, SubType.NUMBER)

# Publish
bus.publish(TyEv.START(kwargs={"num2": 3, "end_event": end_event}))
# 1. The start_print subscriber is triggered.
# --- Output: Start!

# 2. The calc_handler subscriber is triggered. Thanks to default_kwargs in the Handler, it doesn't fail due to a missing num1 argument.
# 3. The got_result ("calc complete") event is published.
# Two subscribers are attached to this event: print_result by NAME and print_txt by ID.
# print_txt is triggered first because the resolution priority is NUMBER -> ID -> NAME (from most specific to least specific).

# 4. The print_txt subscriber is triggered.
# --- Output: {'result': 8, '_func_name': 'calc', '_signal_name': 'calc complete'}
# --- Output: No problems.

# 5. The print_result subscriber is triggered. Thanks to the Handler registration, it doesn't crash from the extra technical metadata arguments. Debug mode adds technical metadata.
# --- Output: 8

# 6. The end_event is published.

# 7. The end_handler subscriber is triggered.
# --- Output: Calculations complete!


# Stop the event bus
bus.stop()

# Stop the calculation thread
logic.calc_thread.join()

```
</details>

## Цитирование

Если вы используете **Smart Event Bus** в своих научных работах, пожалуйста, используйте файл [CITATION.cff](./CITATION.cff).

## Обсуждения

**Остались вопросы или зародилась идея?** Добро пожаловать в [Обсуждения](https://github.com/northsapera/smarteventbus/discussions/1)!