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
from enum import Enum
from typing import Any, Callable, Optional

from .core.config import STACKLEVEL
from .core.custexceptions import (
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
from .core.logictypes import SubscribeType, ThreadType, UniqType
from .core.subscriptionorchestrator import SubscriptionOrchestrator
from .core.threadorchestrator import ThreadOrchestrator

# warnings.filterwarnings("ignore", category=NonValidEventWarning)


# Шина
class EventBus:
    """Шина событий."""

    def __init__(
        self, maxsize: int = 0, maxworkers: Optional[int] = None, paused: bool = False
    ):
        self._queueorch = QueueOrchestrator(maxsize=maxsize)
        self._suborch = SubscriptionOrchestrator()
        self._threadorch = ThreadOrchestrator(maxworkers_sync=maxworkers)

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
        self._queueorch.put(event, event.timeout, event.block)

    def _dispatch(self):
        """Внутренний цикл обработки событий"""
        self._on_air_flag.set()

        while not self._stop_flag.is_set():
            self._can_running_flag.wait()

            if self._stop_flag.is_set():
                break

            try:
                event: Event = self._queueorch.get(timeout=0.1)

                handlers_to_call = self._suborch.get_handlers_snapshot(event)

                for handler in handlers_to_call:
                    context = getattr(handler, "execution_context", ThreadType.POOL)
                    is_async = getattr(
                        handler, "is_async", inspect.iscoroutinefunction(handler)
                    )
                    strict_order = getattr(handler, "strict_order", True)

                    executor = self._threadorch._get_executor_for_context(
                        context=context, is_async=is_async, handler=handler
                    )

                    future = executor.submit(self._run_handler, handler, event)

                    if strict_order:
                        try:
                            future.result()
                        except Exception as e:
                            warnings.warn(
                                f"[EventBus] Infrastructure strict wait failed: {e}",
                                UnpredictableBusWarning,
                            )

                # self._queue.task_done()

            except QueueEmpty:
                continue

            except QueueReset:
                return

            except Exception:
                # self._queue.task_done()

                raise

        self._on_air_flag.clear()

    def _run_handler(self, handler: Handler | Callable, event: Event) -> Any:
        """
        Выполняется внутри целевого экзекутора.
        Универсально вызывает как голые функции, так и объекты Handler.
        """
        try:
            result = handler(*event.args, **event.kwargs)

            if inspect.iscoroutine(result):

                async def async_error_wrapper(coro):
                    try:
                        return await coro
                    except Exception as ax_e:
                        self._handle_processing_error(handler, event, ax_e)

                return async_error_wrapper(result)

            return result

        except Exception as e:
            self._handle_processing_error(handler, event, e)

    def _handle_processing_error(self, handler: Callable, event: Event, e: Exception):
        handler_error_event = TyEv.BUS_ERROR(
            kwargs={
                "txt": f"Ошибка через шину.\nСобытие: {event}.\nОшибка обработчика: {Handler.get_handler_name(handler)}: {type(e).__name__} - {e}",
            }
        )

        if event.id != handler_error_event.id:
            try:
                self.publish(handler_error_event)
                warnings.warn(
                    f"Handler '{Handler.get_handler_name(handler)}' on event (type='{event.type}', name='{event.name}', meta='{event.meta}') ended with error: {type(e).__name__} - {e}",
                    HandlerWarning,
                    stacklevel=STACKLEVEL,
                )

            except QueueFull:
                warnings.warn(
                    f"Bus error event (name='{event.name}', id='{event.id}', num='{event.num}') did not added to the queue! Queue is full!",
                    QueueFullWarning,
                    stacklevel=STACKLEVEL,
                )

        else:
            warnings.warn(
                f"Bus error event (name='{event.name}', id={event.id}, num={event.num}) is ended with error! Error: {type(e).__name__} - {e}",
                UnpredictableBusWarning,
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
