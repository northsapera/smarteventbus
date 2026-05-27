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

"""An threads dispatcher focused on thread safety."""

import asyncio
import inspect
import threading
import time
import warnings
from collections.abc import Iterable
from concurrent.futures import Future, ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeoutError
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

from ..utils.flatten import FlatDict
from .config import STACKLEVEL
from .custexceptions import (
    CallTimeoutError,
    CannotEnd,
    ExecutorError,
    ExecutorInitError,
    PotentialLoop,
    ThreadingsError,
    UnknownExecutorConfig,
)
from .custwarnings import (
    ExecutorWarning,
    HandlerWarning,
    PotentialLoopWarning,
    UnpredictableBusWarning,
)
from .eventclasses import Event, TyEv
from .eventparent import TokenContext
from .handlerclasses import Handler
from .logictypes import PubType, ThreadType
from .publishback import PubBackPool


class AsyncLoopExecutor:
    """
    Исполнитель, запускающий собственный Event Loop в выделенном потоке
    для потокобезопасного выполнения асинхронных хэндлеров.
    """

    def __init__(
        self, max_workers: Optional[int] = None, thread_name_prefix: str = "AsyncWorker"
    ):
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(
            target=self._run_loop, name=thread_name_prefix, daemon=True
        )
        self._thread.start()

        if max_workers is not None:
            warnings.warn(
                "The maxsize of workers was passed during initialization `AsyncLoopExecutor`. The class `AsyncLoopExecutor` is asynchronous, so the maxsize of workers is not taken into account.",
                ExecutorWarning,
            )

    def _run_loop(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    def submit(self, fn: Callable, *args, **kwargs) -> Future:
        """
        Принимает функцию-обертку (например, твой _run_handler_safe).
        Если внутри нее вызывается асинлексика хэндлера, она будет выполнена в Event Loop.
        Мгновенно возвращает стандартный concurrent.futures.Future.
        """
        return asyncio.run_coroutine_threadsafe(
            self._execute(fn, *args, **kwargs), self._loop
        )

    async def _execute(self, fn: Callable, *args, **kwargs) -> Any:
        # Если переданная функция сама является корутиной
        result = fn(*args, **kwargs)
        if inspect.iscoroutine(result):
            return await result
        # Если это обычная функция, которая внутри себя вызывает/обрабатывает корутины
        return result

    def shutdown(self, wait: bool = True) -> None:
        """Корректная остановка цикла событий и завершение потока"""
        if self._loop.is_running():
            self._loop.call_soon_threadsafe(self._loop.stop)
        if wait and self._thread.is_alive():
            self._thread.join()


class ThreadOrchestrator:
    # region dataclasses

    @dataclass
    class ExecutorStorage:
        shared: Optional[ThreadPoolExecutor | AsyncLoopExecutor] = field(default=None)
        dedicated: dict[Any, ThreadPoolExecutor | AsyncLoopExecutor] = field(
            default_factory=lambda: {}
        )
        _publications: Optional[ThreadPoolExecutor] = field(default=None)
        _call_managers: Optional[ThreadPoolExecutor | AsyncLoopExecutor] = field(
            default=None
        )

    # endregion dataclasses

    def __init__(
        self,
        pubback_pool: PubBackPool,
        maxworkers_sync: Optional[int] = None,
    ):
        self._max_workers_sync = maxworkers_sync
        self._pubback_pool = pubback_pool

        self._lock = threading.Lock()

        self._sync_executors = self.ExecutorStorage()
        self._async_executors = self.ExecutorStorage()

        self._exec_storages = [self._sync_executors, self._async_executors]

    def start(self):
        self._sync_executors.shared = ThreadPoolExecutor(
            max_workers=self._max_workers_sync, thread_name_prefix="BusSharedSyncPool"
        )
        self._async_executors.shared = AsyncLoopExecutor(
            thread_name_prefix="BusSharedAsyncPool"
        )

        self._sync_executors._publications = ThreadPoolExecutor(
            max_workers=None, thread_name_prefix="_PublicationsPool"
        )

    def stop(self):
        for storage in self._exec_storages:
            if storage.shared:
                storage.shared.shutdown(wait=True)
                storage.shared = None

            with self._lock:
                for exec_pool in storage.dedicated.values():
                    exec_pool.shutdown(wait=True)
                storage.dedicated.clear()

    def _get_executor_for_context(
        self,
        context: ThreadType = ThreadType.POOL,
        is_async: bool = False,
        handler: Any = None,
    ) -> ThreadPoolExecutor | AsyncLoopExecutor:
        """
        Фабричный метод, возвращающий нужный экзекутор на основе политик хэндлера.
        """
        if not is_async:
            return self._choose_pool(
                context, handler, self._sync_executors, ThreadPoolExecutor
            )

        else:
            return self._choose_pool(
                context, handler, self._async_executors, AsyncLoopExecutor
            )

    def _choose_pool(
        self,
        context: ThreadType,
        handler: Any,
        storage: ExecutorStorage,
        exec_class: type,
    ) -> Any:
        if context is ThreadType.POOL:
            if not storage.shared:
                raise ExecutorInitError(
                    f"Executor {context} in {storage} is not initialized!"
                )
            return storage.shared

        elif context is ThreadType.DEDICATED:
            with self._lock:
                if handler not in storage.dedicated:
                    storage.dedicated[handler] = exec_class(
                        max_workers=1,
                        thread_name_prefix=f"Dedicated-{Handler.get_handler_name(handler)}",
                    )
                return storage.dedicated[handler]

        elif context is ThreadType.INTERNAL_PUBLICATIONS:
            if not storage._publications:
                raise ExecutorInitError(
                    f"Executor {context} in {storage} is not initialized!"
                )
            return storage._publications

        else:
            raise UnknownExecutorConfig(f"Unknown execution context: {context}")

    def iter_handlers(
        self, handlers_to_call: list[Handler | Callable], event: Event
    ) -> None:
        _futures: list[Future] = []

        event_context = event.token.read_content()
        pub_type = event_context["type"]

        for handler in handlers_to_call:
            handler_context = getattr(handler, "execution_context", ThreadType.POOL)
            is_async = getattr(
                handler, "is_async", inspect.iscoroutinefunction(handler)
            )
            strict_order = getattr(handler, "strict_order", True)

            executor = self._get_executor_for_context(
                context=handler_context,
                is_async=is_async,
                handler=handler,
            )

            if not is_async:
                future = executor.submit(
                    self._run_sync_handler,
                    handler=handler,
                    event=event,
                    event_context=event_context,
                )
            else:
                future = executor.submit(
                    self._run_async_handler,
                    handler=handler,
                    event=event,
                    event_context=event_context,
                )

            _futures.append(future)

            if strict_order:
                try:
                    result = future.result(
                        timeout=getattr(handler, "strict_order_timeout", 10)
                    )  # FIXME: Пока так, затем введу обязательную регистрацию и может поменяю
                except Exception as e:
                    warnings.warn(
                        f"Bus infrastructure strict wait failed: {e}",
                        UnpredictableBusWarning,
                    )

        # Если тип публикации - call, все фьючеры собираются в один список и отправляются в менеджер, который в отдельном потоке дождется завершения всех и отправит в переданный фьючер паблишера
        if pub_type == PubType.CALL:
            call_future: Future | None = event_context["content"].get("future", None)
            call_timeout: float | None = event_context["content"].get("timeout", 10)

            if call_future:
                call_executor = self._get_executor_for_context(
                    ThreadType.DEDICATED, False, self._call_manager
                )
                call_executor.submit(
                    self._call_manager,
                    call_future=call_future,
                    _futures_list=_futures,
                    call_timeout=call_timeout,
                )

        try:
            event.token.complete()
        except CannotEnd:
            pass

    def _run_sync_handler(
        self,
        handler: Handler | Callable,
        event: Event,
        event_context: TokenContext,
    ) -> Any:
        try:
            result = handler(*event.args, **event.kwargs)
            self._events_to_pool(result, handler, event, event_context)

            return result

        except Exception as e:
            self._handle_processing_error(handler, event, e, event_context)
            return e

    async def _run_async_handler(
        self,
        handler: Handler | Callable,
        event: Event,
        event_context: TokenContext,
    ) -> Any:
        try:
            result = await handler(*event.args, **event.kwargs)
            self._events_to_pool(result, handler, event, event_context)

            return result

        except Exception as e:
            self._handle_processing_error(handler, event, e, event_context)
            return e

    def _events_to_pool(
        self,
        result: Any,
        handler: Handler | Callable,
        event: Event,
        event_context: TokenContext,
    ) -> Any:
        """
        Обратная отправка в пул событий из полученного результата.
        """
        if (isinstance(handler, Handler) and handler.allow_pubback) or not isinstance(
            handler, Handler
        ):
            if isinstance(result, Event):
                self._pubback_pool.pubback(result, event_context)
            elif isinstance(result, Iterable) and not isinstance(result, (str, bytes)):
                items = result.values() if isinstance(result, dict) else result
                for x in items:
                    if isinstance(x, Event):
                        self._pubback_pool.pubback(x, event_context)

    def _handle_processing_error(
        self,
        handler: Callable,
        event: Event,
        e: Exception,
        failed_event_context: TokenContext,
    ) -> None:
        event.token.error()

        handler_error_event = TyEv.BUS_ERROR(  # TODO: перевести на .model_construct()
            meta=FlatDict(source="bus", type="handler"),
            kwargs={
                "txt": f"Ошибка через шину.\nСобытие: {event}.\nОшибка обработчика: {Handler.get_handler_name(handler)}: {type(e).__name__} - {e}",
            },
        )  # TODO: Перенести создание ошибки внутрь pubback модуля

        if isinstance(e, PotentialLoop):
            warnings.warn(
                f"Handler '{Handler.get_handler_name(handler)}' on event (type='{event.type}', name='{event.name}', meta='{event.meta}') ended with potential pubback loop error: {type(e).__name__} - {e}",
                PotentialLoopWarning,
                stacklevel=STACKLEVEL,
            )
        else:
            warnings.warn(
                f"Handler '{Handler.get_handler_name(handler)}' on event (type='{event.type}', name='{event.name}', meta='{event.meta}') ended with error: {type(e).__name__} - {e}",
                HandlerWarning,
                stacklevel=STACKLEVEL,
            )

        try:
            self._pubback_pool.pubback_error(handler_error_event, failed_event_context)

        except PotentialLoop:
            warnings.warn(
                f"Bus error event (name='{event.name}', id={event.id}, num={event.num}) with token history '{failed_event_context['history']}' wasn't added to the pubback pool! Error: {type(e).__name__} - {e}",
                PotentialLoopWarning,
                stacklevel=STACKLEVEL,
            )

    def _call_manager(
        self,
        call_future: Future,
        _futures_list: list[Future],
        call_timeout: float | None,
    ):  # TODO: Проверить количество создаваемых dedicated потоков, может быть превратить в async с 1 потоком
        results = []
        gen_timeout = call_timeout if call_timeout is not None else 0 + 1
        start_time = time.perf_counter()

        try:
            for _future in _futures_list:
                elapsed = time.perf_counter() - start_time
                remaining_timeout = gen_timeout - elapsed

                if remaining_timeout <= 0 and call_timeout is not None:
                    raise CallTimeoutError("The call() method timed out!")

                result = _future.result(
                    remaining_timeout
                )  # FIXME: Учесть потенциальную двойную публикацию результата
                results.append(result)

            call_future.set_result(tuple(results))

        except CallTimeoutError as e:
            call_future.set_exception(e)

        except FutureTimeoutError:
            call_future.set_exception(CallTimeoutError("The call() method timed out!"))

        except Exception as e:
            call_future.set_exception(e)
