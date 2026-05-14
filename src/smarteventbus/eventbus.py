#!/usr/bin/env python3
#    Copyright 2026 Matvey Aleksandrovich Grigoryev

#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at

#        http://www.apache.org/licenses/LICENSE-2.0

#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.

"""The main dispatcher (conductor), ensuring the interaction of all elements of the system. Main bus."""

import asyncio
import queue
import threading
import time
import warnings
from typing import Callable

from .core.config import STACKLEVEL
from .core.eventclasses import Event, TyEv
from .core.eventqueue import UniquePriorityQueue
from .core.exceptions import (
    TypesInconsistency,
    UnknownEventDataType,
    UnknownSubscribeType,
)
from .core.handlerclasses import Handler
from .core.logictypes import SubscribeType, UniqType
from .core.subscriptionlogic import SubscriptionStorage
from .core.warnings import (
    HandlerWarning,
    NonValidEventWarning,
    QueueFullWarning,
    SubscribeTypeWarning,
    UnpredictableBusWarning,
)

warnings.filterwarnings("ignore", category=NonValidEventWarning)


# Шина
class EventBus:
    """Шина событий."""

    def __init__(self, maxsize: int = 0):
        self._queue = UniquePriorityQueue(maxsize=maxsize)
        self._subscribers: dict[SubscribeType, SubscriptionStorage] = {
            SubscribeType.NAME: {"lists": {}, "id_sets": {}},
            SubscribeType.ID: {"lists": {}, "id_sets": {}},
            SubscribeType.NUMBER: {"lists": {}, "id_sets": {}},
        }
        self._lock = threading.Lock()
        self._stop_flag = threading.Event()
        self._pause_flag = threading.Event()
        self._on_air_flag = threading.Event()
        self._thread = None

    def subscribe(
        self,
        event_data: str
        | Event
        | TyEv
        | type[Event]
        | int,  # FIXME: Перевести на любой Enum
        handlers: Callable | Handler | list[Callable | Handler],
        subscribe_type: SubscribeType = SubscribeType.NAME,
    ):
        """Оформление подписки на событие. Варианты подписки: по имени события (сигнал), по id (тип, имя, метаданные), по порядковому номеру. Хэндлеры активируются последовательно в порядке передачи.

        Args:
            event_data (str | Event | TyEv | type[Event] | int): Текст сигнала | Экземпляр события | Типовое событие | Именованное событие | Номер id или порядковый номер.
            handlers (Callable | Handler | list[Callable | Handler]): Подписчики (функции | хэндлеры).
            subscribe_type (SubscribeType, optional): Тип подписки (`NAME` | `ID` | `NUMBER`). Defaults to SubscribeType.NAME.

        Raises:
            UnknownSubscribeType: Если передан неизвестный тип подписки.
            UnknownEventDataType: Если передан неизвестный тип данных события.
            EventError: Если у именнованного события не определено имя.
            TypesInconsistency: Если тип подписки не соответствует возможностям переданных данных события или если в качестве хэндлера передан non-callable объект.

        Notes:
            - Подписка по `NAME` доступна для **любого типа** передаваемых **данных** события.

            -> Наиболее широкий тип подписки, подписчик активируется от *любого события*, имеющего переданное **имя** (сигнал).

            - Подписка по `ID` доступна для: **экземляра события** (`Event`), **типового события** (`TyEv`), **именнованного события** (`type[Event]`), **номера id** (`int`).

            -> Подписчик активируется при *полном* совпадении **типа** события (класс), **имени** события (сигнал) и **метаданных** события.

            - Подписка по `NUMBER` доступна для: **экземляра события** (`Event`), **номера num** (`int`).

            -> Порядковый номер *уникален* для каждого события и *никогда не меняется*, подписка на *конкретный объект*.

            - Для отказоустойчивости предпочтительно при инициализации хэндлеров указывать в аргументах *args, **kwargs.

        Examples:
            Подготовка: Создаем и запускаем шину, создаем хэндлеры:

            >>> bus = EventBus()
            >>> bus.start()

            >>> def start(*args, **kwargs):
            ...     print("Начинаем...")

            >>> def stop(*args, **kwargs):
            ...     print("Останавливаем...")

            >>> def make_report(txt: str):
            ...     print(f"Отчет: {txt}")

            >>> report = Handler(func=make_report, default_kwargs={"txt": "notfound"})

            >>> def close(*args, **kwargs):
            ...     print("Конец.")

            Подписка по NAME:

            >>> bus.subscribe("start", start)

            Подписка по ID:

            >>> bus.subscribe(Event(name="report", meta={"source": "cycle"}), report, SubscribeType.ID)
            >>> bus.subscribe(TyEv.STOP, [stop, close], SubscribeType.ID)

            Подписка по номеру события ниже.

            Публикация событий (time.sleep с запасом, чтобы шина успевала прокидывать события для доктеста):

            >>> bus.publish(Event(name="start"))
            >>> time.sleep(0.1)
            Начинаем...

            Цикл работы:

            >>> for i in range(2, 5):
            ...     bus.publish(Event(name="report", meta={"source": "cycle"}, kwargs={"txt": f"Текущее событие: {i}"})) # doctest: +ELLIPSIS
            ...     time.sleep(0.1)
            Отчет: Текущее событие: 2
            Отчет: Текущее событие: 3
            Отчет: Текущее событие: 4

            Событие 5:

            >>> event_5 = TyEv.LOG()
            >>> e_5_num: int = event_5.num

            Подписка по номеру события (событие под порядковым номером 5 остановит и закроет вне зависимости от его типа, имени, метаданных):

            >>> bus.subscribe(e_5_num, [stop, close], SubscribeType.NUMBER)

            Публикация события номер 5 логирования:

            >>> bus.publish(event_5)
            >>> time.sleep(0.5)
            Останавливаем...

            Очистка очереди, остановка шины.

            >>> bus.clean_queue()
            >>> bus.stop()
        """
        signal = None
        handlers = [handlers] if isinstance(handlers, Callable) else handlers

        if isinstance(event_data, (str, int)):
            if subscribe_type == SubscribeType.NAME:
                if isinstance(event_data, int):
                    warnings.warn(
                        f"Event data '{event_data}' type is `int`. Event data must be `str` type for NAME logic! Event data transformed.",
                        SubscribeTypeWarning,
                        stacklevel=STACKLEVEL,
                    )

                signal = str(event_data)

            elif subscribe_type in {SubscribeType.ID, SubscribeType.NUMBER}:
                if isinstance(event_data, str):
                    warnings.warn(
                        f"Event data '{event_data}' type is `str`. Event data must be `int` type for ID and NUMBER logic! Event data transformed.",
                        SubscribeTypeWarning,
                        stacklevel=STACKLEVEL,
                    )

                try:
                    signal = int(event_data)

                except ValueError:
                    raise TypesInconsistency(
                        f"Cannot convert string '{event_data}' to 128-bit int for {subscribe_type.name}."
                    )

            else:
                raise UnknownSubscribeType("Unknown subscription type received!")

        elif isinstance(event_data, Event):
            try:
                mapping = {
                    SubscribeType.NAME: event_data.name,
                    SubscribeType.ID: event_data.id,
                    SubscribeType.NUMBER: event_data.num,
                }
                signal = mapping[subscribe_type]
            except KeyError:
                raise UnknownSubscribeType("Unknown subscription type received!")

        elif isinstance(event_data, (TyEv, type)):  # FIXME
            default_data = Event.get_default_data(event_data)
            e_type, name, meta = default_data

            if subscribe_type == SubscribeType.NAME:
                signal = name
            elif subscribe_type == SubscribeType.ID:
                signal = Event.get_id(*default_data)
            else:
                raise UnknownSubscribeType(
                    f"Cannot use {subscribe_type} with class/enum types."
                )

        else:
            raise UnknownEventDataType("Unknown event data type received!")

        with self._lock:
            if signal is None:
                raise TypesInconsistency(
                    "Event data must comply with the SubscribeType. logic!"
                )

            subscribers_list = self._subscribers[subscribe_type]["lists"].setdefault(
                signal, []
            )

            subscribers_id_set = self._subscribers[subscribe_type][
                "id_sets"
            ].setdefault(signal, set())

            for handler in handlers:
                if not callable(handler):
                    raise TypesInconsistency(
                        f"Each handler must be callable! Handler '{handler}' is not."
                    )

                h_id = id(handler)

                if h_id not in subscribers_id_set:
                    subscribers_list.append(handler)
                    subscribers_id_set.add(h_id)

    def publish(self, event: Event):
        """Публикует событие в шину.

        Args:
            event (Event): Экземпляр события. Поддерживается передача типовых и нетиповых событий. При передаче событий доступно изменение любых открытых параметров, включая параметры типовых событий.


        Examples:
            Подготовка: Создаем и запускаем шину, создаем хэндлеры:

            >>> bus = EventBus()
            >>> bus.start()

            >>> def start(*args, **kwargs):
            ...     print("Начинаем...")

            >>> def stop(*args, **kwargs):
            ...     print("Останавливаем...")

            >>> def make_report(txt: str):
            ...     print(f"Отчет: {txt}")

            >>> report = Handler(func=make_report, default_kwargs={"txt": "notfound"})

            Оформление подписок:

            >>> bus.subscribe(TyEv.START, [start, report])
            >>> bus.subscribe(Event(name="stop", meta={"source": "gui"}), [stop, report])

            Для публикации типового события:

            >>> bus.publish(TyEv.START())
            >>> time.sleep(0.1)
            Начинаем...
            Отчет: Старт.

            Для публикации нетипового события:

            >>> bus.publish(Event(name="stop", meta={"source": "gui"}, kwargs={"txt": "Останавливающее событие."}, priority=50, uniq_type=UniqType.NONE, timeout=None))
            >>> time.sleep(0.1)
            Останавливаем...
            Отчет: Останавливающее событие.

            Очистка очереди, остановка шины.

            >>> bus.clean_queue()
            >>> bus.stop()

        """
        self._queue.put(event)

    def _dispatch(self):
        """Внутренний цикл обработки событий"""
        self._on_air_flag.set()

        while not self._stop_flag.is_set():
            if not self._pause_flag.is_set():
                try:
                    event: Event = self._queue.get(timeout=0.1)
                    with self._lock:
                        handlers_to_call = []

                        signals = {
                            SubscribeType.NUMBER: event.num,
                            SubscribeType.ID: event.id,
                            SubscribeType.NAME: event.name,
                        }

                        for sub_type, signal in signals.items():
                            handlers = self._subscribers[sub_type]["lists"].get(signal)
                            if handlers:
                                handlers_to_call.extend(handlers.copy())

                    for handler in handlers_to_call:
                        try:
                            handler(*event.args, **event.kwargs)

                        except Exception as e:
                            handler_error_event = TyEv.BUS_ERROR(
                                kwargs={
                                    "txt": f"Ошибка через шину.\nСобытие: {event}.\nОшибка обработчика: {handler}: {e}",
                                }
                            )
                            if event.id != handler_error_event.id:
                                try:
                                    self.publish(handler_error_event)
                                    warnings.warn(
                                        f"Handler on event\n'{event}\n'{handler}' ended with error: {e}",
                                        HandlerWarning,
                                        stacklevel=STACKLEVEL,
                                    )
                                except queue.Full:
                                    warnings.warn(
                                        f"Bus error event (name='{event.name}', id={event.id}, num={event.num}) did not added to the queue! Queue is full!",
                                        QueueFullWarning,
                                        stacklevel=STACKLEVEL,
                                    )
                            else:
                                warnings.warn(
                                    f"Bus error event (name='{event.name}', id={event.id}, num={event.num}) is ended with error! Error: {e}",
                                    UnpredictableBusWarning,
                                    stacklevel=STACKLEVEL,
                                )

                    self._queue.task_done()

                except queue.Empty:
                    continue

                except Exception:
                    self._queue.task_done()

                    raise

        self._on_air_flag.clear()

    def clean_queue(self, *args, **kwargs):
        self._pause_flag.set()
        self._queue.clean_queue()
        self._pause_flag.clear()

    def start(self):
        """Запуск диспетчера в отдельном потоке"""
        if not self._on_air_flag.is_set():
            self._stop_flag.clear()
            self._pause_flag.clear()

            self._thread = threading.Thread(target=self._dispatch, daemon=True)
            self._thread.start()

    def stop(self):
        """Остановка шины"""
        self._stop_flag.set()

        if self._thread:
            self._thread.join()

    def report(self) -> dict:
        with self._lock:
            subscribers = {
                t.value: {
                    "lists": {
                        i: [Handler.get_handler_name(h) for h in h_group]
                        for i, h_group in t_group["lists"].items()
                    },
                    "id_sets": {i: list(s) for i, s in t_group["id_sets"].items()},
                }
                for t, t_group in self._subscribers.items()
            }

            report = {
                "subscribers": subscribers,
                "queue_info": self._queue.info(),
            }
        return report


bus = EventBus()
"""Базовая шина."""


class BusNetwork:
    """Базовый класс для всех классов с подключением к bus."""

    bus = bus


def main(txt: str):
    print(txt)


if __name__ == "__main__":
    import doctest

    doctest.testmod(optionflags=doctest.ELLIPSIS, verbose=True)

    # doctest.run_docstring_examples(
    #     Handler.duble, globals(), optionflags=doctest.ELLIPSIS, verbose=True
    # )
