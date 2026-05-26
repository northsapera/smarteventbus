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

"""An event dispatcher focused on thread safety."""

import threading
import warnings

from .config import STACKLEVEL, debug_mode
from .custexceptions import (
    QueueEmpty,
    QueueError,
    QueueFull,
    QueueReset,
    StopTimeoutError,
    UnknownExitType,
    WaitTimeoutError,
)
from .custwarnings import NonValidEventWarning, QueueResetWarning, WaitTimeoutWarning
from .eventclasses import Event
from .eventqueue import UniquePriorityQueue
from .logictypes import ExitType


class QueueOrchestrator:
    def __init__(self, maxsize: int = 0):
        self._lock = threading.Lock()
        self._consumer_condition = threading.Condition(lock=self._lock)
        self._producer_condition = threading.Condition(lock=self._lock)

        self.maxsize = maxsize

        self._queue = UniquePriorityQueue()

        self._reset = False

    def put(
        self,
        event: Event,
        timeout: None | float = None,
        block: bool = True,
        _no_wait: bool = False,
    ) -> None:
        def queue_is_free():
            return (
                not self.maxsize > 0 or self._queue.qsize < self.maxsize or self._reset
            )

        with self._lock:
            timeout = timeout if block and not _no_wait else 0

            success = self._producer_condition.wait_for(
                lambda: self._queue.can_put(event) and queue_is_free(),
                timeout=timeout,
            )

            if self._reset:
                warnings.warn(
                    f"Query reset, event {event.name} was rejected.", QueueResetWarning
                )
                return

            if not success:
                if (self.maxsize > 0 and self._queue.qsize >= self.maxsize) or _no_wait:
                    raise QueueFull(
                        f"The event '{event.name}' was rejected by full queue!"
                    )

                if event.wait_timeout_exit == ExitType.REJECT:
                    self._queue.inspection.wait_errors_amount()
                    raise WaitTimeoutError(
                        f"The WAIT logic event '{event.name}' was rejected by timeout ({timeout})!"
                    )

                elif event.wait_timeout_exit == ExitType.PUT:
                    self._queue.inspection.wait_warnings_amount()
                    warnings.warn(
                        f"Event '{event.name}' WAIT timeout exceeded. Forcing PUT into queue.",
                        WaitTimeoutWarning,
                        stacklevel=STACKLEVEL,
                    )

                else:
                    raise UnknownExitType("Unknown exit logic type received!")

            self._queue.put(event)

            self._consumer_condition.notify()

    def get(
        self, timeout: None | float = None, block: bool = True, _no_wait: bool = False
    ) -> Event:
        def queue_is_available():
            return self._queue.qsize > 0 or self._reset

        with self._lock:
            timeout = timeout if block and not _no_wait else 0

            success = self._consumer_condition.wait_for(
                queue_is_available, timeout=timeout
            )

            if self._reset:
                raise QueueReset("Query reset, getting was rejected.")

            if not success:
                raise QueueEmpty(f"Queue is empty by timeout ({timeout})!")

            event = self._queue.get()

            self._producer_condition.notify_all()

            return event

    def put_no_wait(self, event: Event) -> None:
        self.put(event, _no_wait=True)

    def get_no_wait(self) -> Event:
        return self.get(_no_wait=True)

    def clean_queue(self) -> None:
        with self._lock:
            self._queue.clean_queue()

            self._producer_condition.notify_all()

    def reset_queue(self) -> None:
        with self._lock:
            self._reset = True
            self._queue.clean_queue()

            self._producer_condition.notify_all()
            self._consumer_condition.notify_all()

    def task_done(self) -> None:
        with self._lock:
            self._queue.task_done()

            self._producer_condition.notify_all()
            self._consumer_condition.notify_all()

    def join(self, timeout: float = 10.0) -> None:
        with self._lock:
            success = self._producer_condition.wait_for(
                lambda: self._queue.is_joined, timeout=timeout
            )

            if not success:
                raise StopTimeoutError("Stop timeout has gone!")

            self._producer_condition.notify_all()
            self._consumer_condition.notify_all()

    @property
    def qsize(self) -> int:
        with self._lock:
            return self._queue.qsize

    @property
    def info(self) -> dict:
        with self._lock:
            return self._queue.info
