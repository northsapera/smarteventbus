![Python Version](https://img.shields.io/badge/python-3.10%2B-darkblue?style=flat-square) ![License](https://img.shields.io/badge/license-Apache%202.0-darkgreen?style=flat-square) ![Status](https://img.shields.io/badge/status-beta-yellow?style=flat-square)
# Smart Event Bus
## Краткое описание
Эта шина событий создана для **безопасного** управления *событиями* с ориентированием на **многопоточную** среду. Работает по принципу *издатель-шина-подписчик*.
### Особенности шины
Особенностью шины является **потокобезопасность** и **сложная логика работы с событиями** и **очередью событий**. Поддерживает возможность расстановки по **приоритету**, **контроля** и **учета** событий в очереди, **замены** и **отключения** событий, синхронизации потоков через **работу с наполнением** очереди, публикации событий из **любого потока**.
### Почему Smart?
Философия созданной шины заключается в изменении подхода к системе событий - вместо попытки как можно быстрее пробросить их сквозь, эта шина берет роль **диспетчера**, которому можно задать логику и условия взаимодействия событий между собой и окружением. Это позволяет **гибко и масштабируемо** использовать шину в разных условиях - от медленных и ответственных процессов, где нельзя позволить себе потерять данные из-за ошибки, до динамической визуальной отрисовки, где нужна скорость, но есть возможность безболезненно "опустить" часть информации. **Низкая связность**, **приоритетная очередь**, использование **логики уникальности**, работа с обособленными событиями, строгая типизация через **контейнеры** и **потокобезопасность** делает эту шину универсально применимой в **большинстве** ситуаций. Подробнее см. [*'Логика работы'*](#логика-работы) и [*'Примеры использования'*](#примеры-использования).
### Логика работы
Ядром шины является **приоритетная потокобезопасная очередь** с **учетом** вложенных элементов. При публикации событие упаковывается в **строго типизированный контейнер**, основанный на *Pydantic-модели*, и кладется в очередь согласно указанной **логике уникальности**. В данный момент поддерживается три типа:

| Тип (UniqType) | Описание логики | Применение |
| :---: | :--- | :--- |
| **NONE** | Событие просто добавляется в очередь. | Обычные уведомления, где важен каждый факт события. |
| **WAIT** | Ждет, если в ней уже находится *n* идентичных событий. | Синхронизация потоков по самому медленному участнику. |
| **REPLACE** | Заменяет *n-ное* идентичное событие в очереди (с начала). | Оптимизация: обновление прогрессбаров, когда важен только актуальный статус. |

Идентичность событий оценивается по их *типу* (имени класса), *имени* и *метаданным*. События с идентичными типом, именем и метаданными считаются одинаковыми **вне зависимости от наполнения**.

Публикация событий доступна из **любого потока**, активация хэндлеров (подписчиков) происходит в **выделенном под цикл обработки очереди потоке**. В качестве подписчиков выступают **функции** или "зарегистрированные" **хэндлеры** (объект Handler), **защищающие и дополняющие** упакованные функции. В данный момент поддерживается три типа подписки:

| Тип (SubscriptionType) | Критерий активации | Точность |
| :---: | :--- | :---: |
| **NAME** | Совпадение *имени* события. | Низкая (широкий охват) |
| **ID** | Совпадение *типа* (имени класса), *имени* и *метаданных*. | Средняя |
| **NUMBER** | Точное совпадение *порядкового номера*. | Максимальная (адресная связь) |

## Планы развития
Текущая работа заключается в:
- Добавлении поддержки асинхронности
- Добавлении поддержки мультипроцессинга
- Добавлении возможности обратной связи функций
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
### Примеры использования
- Запуск и использование [(короткий пример)](./examples/readme_short_example.py):
```python
import time

from smarteventbus import Event, Handler, bus

bus.start()


def hello(greet, target):
    print(f"{greet}, {target}!")


hello_handler = Handler(func=hello, default_kwargs={"greet": "Hello"})
hello_event = Event(name="greetings", kwargs={"target": "World"})

bus.subscribe(hello_event, hello_handler)
bus.publish(hello_event)
# Hello, World!

time.sleep(1)
bus.stop()

```

<br>

- [Развернутый пример с пояснениями](./examples/readme_detailed_example.py):

<details>
<summary>Подробно</summary>

```python
import threading
import time

from smarteventbus import BusNetwork, Event, Handler, TyEv, UniqType, bus, debug_mode
from smarteventbus import SubscribeType as SubType

# Установка флага отладки
debug_mode.set()

# Запуск шины
bus.start()


# Создание классов с подключением к шине (в случае необходимости публикации событий)
class Logic(BusNetwork):
    def __init__(self):
        self.bus = super().bus

    def calc(self, num1: int, num2: int, end_event: Event):
        result = num1 + num2

        # Создание события (имя события, передаваемые данные, приоритет, логика уникальности)
        got_result = Event(
            name="calc complete",
            kwargs={"result": result},
            priority=10,
            uniq_type=UniqType.WAIT,
        )

        # Публикация результата
        self.bus.publish(got_result)
        self.bus.publish(end_event)

    def calc_to_thread(self, num1: int, num2: int, end_event: Event):
        """Запуск расчетов в отдельном потоке"""
        self.calc_thread = threading.Thread(
            target=self.calc,
            kwargs={"num1": num1, "num2": num2, "end_event": end_event},
            daemon=True,
        )

        self.calc_thread.start()


logic = Logic()

""" - В основном потоке: - """


def start_print(**kwargs):
    print("Start!")


def print_result(result: int, **kwargs):
    print(result)


def print_txt(**kwargs):
    print(kwargs)
    print("No problems.")


def end_calc():
    print("Calculations complete!")


# Создание подписчиков
calc_handler = Handler(func=logic.calc_to_thread, default_kwargs={"num1": 5})
end_handler = Handler(func=end_calc)

# Подписка
# TyEv.START - типовое событие, имеет значения по умолчанию, допускает их изменение тем же образом, что и при создании нового события
bus.subscribe(
    TyEv.START, [start_print, calc_handler]
)  # Указание нескольких подписчиков списком гарантирует порядок их активации

# Если подписчик - обычная функция, рекомендуется в ее аргументах указывать **kwargs, так как шина пробрасывает события "как есть" со всеми переданными и внутренними (при DEBUG_MODE) аргументами
bus.subscribe("calc complete", print_result)

# Подписка по ID не принимает строку
bus.subscribe(Event(name="calc complete"), print_txt, SubType.ID)

# Подписка по NUMBER принимает конкретный объект или номер
end_event = TyEv.END()
bus.subscribe(end_event, end_handler, SubType.NUMBER)

# Публикация
bus.publish(TyEv.START(kwargs={"num2": 3, "end_event": end_event}))
# Активируется подписчик start_print
# --- Вывод: Start!

# Активируется подписчик calc_handler, благодаря default_kwargs в хэндлере не падает с ошибкой от отсутствия num1
# Публикация события got_result ("calc complete")
# К этому событию привязаны 2 подписчика: print_result по NAME и print_txt по ID
# Первым активируется print_txt, так как порядок NUMBER->ID->NAME (от более точного к менее)

# Активируется подписчик print_txt, благодаря регистрации в Handler не падает от лишнего аргумента result. Благодаря файлу отладки добавляется техническая информация
# --- Вывод: {'result': 8, '_func_name': 'calc', '_signal_name': 'calc complete'}
# --- Вывод: No problems.

# Активируется подписчик print_result
# --- Вывод: 8

# Публикация события end_event

# Активируется подписчик end_handler
# --- Вывод: Calculations complete!

# Небольшое ожидание, чтобы поток calc_thread успел создаться (в обычной ситуации не требуется)
time.sleep(1)

# Остановка потока расчетов
logic.calc_thread.join()

# Остановка шины
bus.stop()

```
</details>

## Цитирование
Если вы используете **Smart Event Bus** в своих научных работах, пожалуйста, используйте файл [CITATION.cff](./CITATION.cff).

## Обсуждения
**Остались вопросы или зародилась идея?** Добро пожаловать в [Обсуждения](https://github.com/northsapera/smarteventbus/discussions/1)!