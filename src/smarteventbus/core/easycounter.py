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
