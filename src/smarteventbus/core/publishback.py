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

"""Publish events back pool and methods."""

import copy
from threading import Lock

from ..utils.flatten import FlatDict
from .custexceptions import PotentialLoop
from .eventclasses import Event
from .eventparent import TokenContext
from .logictypes import PubType


class PubBackPool:
    """Хранилище событий для обратной публикации"""

    def __init__(self):
        self._lock: Lock = Lock()

        self._pool: list[Event] = []

    def _pubback_init(
        self, event: Event, ttl: int = 6, history: None | tuple[int, ...] = None
    ):
        if history is None:
            history = tuple()

        with self._lock:
            event.token.write_content(
                type=PubType.PUBBACK, content=FlatDict(ttl=ttl), history=history
            )
            self._pool.append(event)

    def _raise_loop(self, event: Event, ttl: int, history: tuple[int, ...]):

        if ttl < 0:
            raise PotentialLoop(
                "Event reverse publication TTL expired, risk of loop, check architecture or increase TTL size in initialization event!"
            )
        if event.id in history:
            raise PotentialLoop("Event ID found in token history, high risk of loop!")

    def pubback_error(
        self, error_event: Event, old_token_context: None | TokenContext = None
    ):
        ttl: int = error_event.pubback_ttl

        if old_token_context is not None:
            history: tuple[int, ...] = old_token_context["history"]

            self._raise_loop(error_event, ttl, history)

            history += (-1, 3, error_event.id)

        else:
            history = (-1, 3, error_event.id)

        self._pubback_init(error_event, ttl, history)

    def pubback(
        self, event: Event, old_token_context: TokenContext
    ):  # Сделать проверку на WAIT логику (опасно!)
        ttl: int = old_token_context["content"].get("ttl", event.pubback_ttl) - 1
        history: tuple[int, ...] = old_token_context["history"]

        self._raise_loop(event, ttl, history)

        history += (3, event.id)

        self._pubback_init(event, ttl, history)

    def extract(self) -> list[Event]:
        with self._lock:
            extracting = copy.copy(self._pool)
            self._pool = []

            return extracting

    @property
    def info(self) -> dict:
        with self._lock:
            return {"qsize": len(self._pool)}


global_pubback_pool = PubBackPool()
"""Глобальное общее хранилище"""
