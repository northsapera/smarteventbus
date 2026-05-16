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
import warnings
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

from .custexceptions import (
    ExecutorError,
    ExecutorInitError,
    ThreadingsError,
    UnknownExecutorConfig,
)
from .custwarnings import ExecutorWarning
from .handlerclasses import Handler
from .logictypes import ThreadType


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

    # endregion dataclasses

    def __init__(
        self,
        maxworkers_sync: Optional[int] = None,
    ):
        self._max_workers_sync = maxworkers_sync

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
        self, context: str = "pool", is_async: bool = False, handler: Any = None
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
        self, context: str, handler: Any, storage: ExecutorStorage, exec_class: type
    ) -> Any:
        if context == ThreadType.POOL:
            if not storage.shared:
                raise ExecutorInitError(
                    f"Executor 'pool' in {storage} is not initialized!"
                )
            return storage.shared

        elif context == ThreadType.DEDICATED:
            with self._lock:
                if handler not in storage.dedicated:
                    storage.dedicated[handler] = exec_class(
                        max_workers=1,
                        thread_name_prefix=f"Dedicated-{Handler.get_handler_name(handler)}",
                    )
                return storage.dedicated[handler]

        else:
            raise UnknownExecutorConfig(f"Unknown execution context: {context}")
