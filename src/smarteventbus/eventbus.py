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

import inspect
import queue
import threading
import time
import warnings
from concurrent.futures import Future
from concurrent.futures import TimeoutError as FutureTimeoutError
from enum import Enum
from typing import Any, Callable, Optional

from .core.config import STACKLEVEL
from .core.custexceptions import (
    CallTimeoutError,
    QueueEmpty,
    QueueFull,
    QueueReset,
    TypesInconsistency,
    UnknownEventDataType,
    UnknownSubscribeType,
)
from .core.custwarnings import (
    HandlerWarning,
    NonValidEventWarning,
    QueueFullWarning,
    SubscribeTypeWarning,
    UnpredictableBusWarning,
)
from .core.eventclasses import Event, TyEv
from .core.eventorchestrator import QueueOrchestrator
from .core.eventqueue import UniquePriorityQueue
from .core.handlerclasses import Handler
from .core.logictypes import (  # TODO: Поменять везде с == на is
    PubType,
    SubscribeType,
    ThreadType,
    UniqType,
)
from .core.publishback import PubBackPool
from .core.subscriptionorchestrator import SubscriptionOrchestrator
from .core.threadorchestrator import ThreadOrchestrator
from .utils.flatten import FlatDict

# warnings.filterwarnings("ignore", category=NonValidEventWarning)


# Шина
class EventBus:
    """Шина событий."""

    def __init__(
        self, maxsize: int = 0, maxworkers: Optional[int] = None, paused: bool = False
    ):
        self._pubback_pool = PubBackPool()
        self._queueorch = QueueOrchestrator(maxsize=maxsize)
        self._suborch = SubscriptionOrchestrator()
        self._threadorch = ThreadOrchestrator(
            pubback_pool=self._pubback_pool, maxworkers_sync=maxworkers
        )

        self._stop_flag = threading.Event()
        self._can_running_flag = threading.Event()
        self._on_air_flag = threading.Event()

        self._thread = None

        if not paused:
            self._can_running_flag.set()

    def subscribe(
        self,
        event_data: str
        | Event
        | TyEv
        | Enum
        | type[Event]
        | int,  # NOTE: Перевести на любой Enum (сделано?)
        handlers: Callable | Handler | list[Callable | Handler],
        subscribe_type: SubscribeType = SubscribeType.NAME,
    ):  # TODO: Сделать принудительную регистрацию в Handler
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
        self._suborch.subscribe(event_data, handlers, subscribe_type)

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
        event.token.write_content(
            type=PubType.PUBLISH, content=FlatDict(), history=(0, event.id)
        )
        self._queueorch.put(event, event.timeout, event.block)

    def call(self, event: Event, timeout: float = 10):
        future = Future()

        event.token.write_content(
            type=PubType.CALL,
            content=FlatDict(future=future, timeout=timeout),
            history=(1, event.id),
        )
        self._queueorch.put(event, event.timeout, event.block)

        try:
            result = future.result(timeout=timeout)
        except FutureTimeoutError:
            raise CallTimeoutError("The call() method timed out!")

        return result

    def _dispatch(
        self,
    ):  # TODO: Перевевсти на обработку кэшированных событий и подписчиков - метаданные маршрутизации кэшируются во внутреннем словаре (not urgent)
        """Внутренний цикл обработки событий"""
        self._on_air_flag.set()

        while not self._stop_flag.is_set():
            self._can_running_flag.wait()

            if self._stop_flag.is_set():
                break

            try:
                event: Event = self._queueorch.get(timeout=0.1)

                handlers_to_call = self._suborch.get_handlers_snapshot(event)

                self._threadorch.iter_handlers(handlers_to_call, event)

                # self._queue.task_done()

            except QueueEmpty:
                pass

            except QueueReset:
                return

            except Exception:
                # self._queue.task_done()

                raise

            callback_events: list[Event] = self._pubback_pool.extract()

            if callback_events:
                for callback_event in callback_events:
                    self._pubback_pool_to_queue(callback_event)

        self._on_air_flag.clear()

    def _pubback_pool_to_queue(self, callback_event: Event):
        try:
            self._queueorch.put_no_wait(callback_event)

        except QueueFull:
            warnings.warn(
                f"Bus event (type='{callback_event.type}', name='{callback_event.name}', id='{callback_event.id}', num='{callback_event.num}') did not published back to the queue! Queue is full!",
                QueueFullWarning,
                stacklevel=STACKLEVEL,
            )

    def clean_queue(self, *args, **kwargs):
        self._queueorch.clean_queue()

    def start(self):
        """Запуск диспетчера в отдельном потоке"""
        if not self._on_air_flag.is_set():
            self._stop_flag.clear()

            self._thread = threading.Thread(target=self._dispatch, daemon=True)
            self._thread.start()

            self._threadorch.start()

    def stop(self):
        """Остановка шины"""
        self._stop_flag.set()
        self._can_running_flag.set()

        # Информируем оркестратор очереди, что пора раздать QueueReset всем, кто ждет get()
        self._queueorch.reset_queue()

        if self._thread:
            self._thread.join(timeout=2.0)

        # Тушим пулы потоков в оркестраторе
        self._threadorch.stop()

    def pause(self):
        self._can_running_flag.clear()

    def resume(self):
        self._can_running_flag.set()

    def report(self) -> dict:
        report = {
            "subscribers": self._suborch.info,
            "queue_info": self._queueorch.info,
            "pubback_info": self._pubback_pool.info,
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
