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

"""Event parent with counter and static methods."""

import copy
import hashlib
import itertools
from dataclasses import dataclass, field
from enum import IntEnum
from threading import Lock

from ..utils.flatten import FlatDict
from .custexceptions import CannotComplete, RePublishing, UnReadToken


class EventParent:
    _counter_iterator = itertools.count()

    @staticmethod
    def get_id(e_type: str, e_name: str, e_meta: dict) -> int:
        """Рассчитывает id события"""
        meta_part = str(sorted(e_meta.items())) if e_meta else ""
        raw_key = f"{e_type}:{e_name}:{meta_part}"
        hash_digest = hashlib.md5(raw_key.encode("utf-8")).hexdigest()

        return int(hash_digest, 16) & ((1 << 128) - 1)


class TokenState(IntEnum):
    INITIALIZED = 0
    IN_QUEUE = 1
    IN_WORK = 2
    SUCCESSFULLY_COMPLETED = 3

    DEVALIDED_AFTER_WRITE_TRYING = 101
    DEVALIDED_AFTER_READ_TRYING = 102
    DEVALIDED_AFTER_HANDLER_ERROR = 103
    DEVALIDED_BY_QUEUE = 104
    DEVALIDED_AFTER_FINISH_TRYING = 105
    DEVALIDED_GENERIC = 106

    MANUALLY_DEVALIDED = 1000

    @property
    def readable_name(self) -> str:
        parts = self.name.lower().split("_")
        parts = ["devalidated" if p == "devalided" else p for p in parts]
        return f"{parts[0].capitalize()} {' '.join(parts[1:])}".strip()


@dataclass
class EventToken:
    _lock: Lock = field(default_factory=lambda: Lock(), init=False)

    _state: TokenState = field(default=TokenState.INITIALIZED, init=False)
    _content: FlatDict | None = field(default=None, init=False)

    def write_content(self, content: FlatDict) -> None:
        with self._lock:
            if self._state == TokenState.INITIALIZED:
                self._content = copy.copy(content)
                self._state = TokenState.IN_QUEUE
            else:
                self._content = None
                self._state = TokenState.DEVALIDED_AFTER_WRITE_TRYING
                raise RePublishing(
                    "The event has already been published or is non-valide! Use the '.duble()' for the event object."
                )

    def drop_by_queue(self) -> None:
        with self._lock:
            self._state = TokenState.DEVALIDED_BY_QUEUE
            self._content = None

    def read_content(self) -> FlatDict:
        with self._lock:
            if self._state == TokenState.IN_QUEUE:
                self._state = TokenState.IN_WORK
                result = copy.copy(self._content)
                self._content = None
                return result if result else FlatDict()
            else:
                self._content = None
                self._state = TokenState.DEVALIDED_AFTER_READ_TRYING
                raise UnReadToken("The token content isn't avalaible for reading!")

    def complete(self) -> None:
        with self._lock:
            if self._state == TokenState.IN_WORK:
                self._content = None
                self._state = TokenState.SUCCESSFULLY_COMPLETED
            else:
                self._content = None
                self._state = TokenState.DEVALIDED_AFTER_FINISH_TRYING
                raise CannotComplete("The event cannot be completed at this stage!")

    def error(self) -> None:
        with self._lock:
            if self._state == TokenState.IN_WORK:
                self._content = None
                self._state = TokenState.DEVALIDED_AFTER_HANDLER_ERROR
            else:
                self._content = None
                self._state = TokenState.DEVALIDED_AFTER_FINISH_TRYING
                raise CannotComplete("The event cannot be completed at this stage!")

    def devalid(self) -> None:
        with self._lock:
            self._content = None
            self._state = TokenState.MANUALLY_DEVALIDED

    @property
    def state_num(self) -> int:
        with self._lock:
            return int(self._state)

    @property
    def state_type(self) -> TokenState:
        with self._lock:
            return self._state

    @property
    def state(self) -> str:
        with self._lock:
            return self._state.readable_name

    def __int__(self) -> int:
        return self.state_num

    def __repr__(self) -> str:
        return self.state
