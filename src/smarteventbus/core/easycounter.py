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

from threading import Lock


class EasyCounter:
    """Потокобезопасный счетчик, возвращает значение, увеличенное (уменьшенное) на delta или текущее значение."""

    def __init__(self, initial: int = 0) -> None:
        self._value = initial
        self._lock = Lock()

    def __call__(self, delta: int = 1) -> int:
        return self.inc(delta)

    def inc(self, delta: int = 1) -> int:
        with self._lock:
            self._value += delta
            return self._value

    def cur(self) -> int:
        with self._lock:
            return self._value

    def __int__(self) -> int:
        return self.cur()

    def __repr__(self) -> str:
        return str(self.cur())
