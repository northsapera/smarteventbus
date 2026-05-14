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

import asyncio
import copy
import hashlib
import inspect
import itertools
import queue
import sys
import threading
import time
import types
import warnings
from dataclasses import asdict, dataclass, field
from enum import Enum
from functools import total_ordering
from typing import (
    Annotated,
    Any,
    Callable,
    Dict,
    Final,
    Iterable,
    List,
    Optional,
    Self,
    TypedDict,
    cast,
    get_args,
    get_origin,
    get_type_hints,
)

from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    GetCoreSchemaHandler,
    PrivateAttr,
    ValidationError,
    model_validator,
)
from pydantic_core import PydanticUndefined, core_schema
from typing_extensions import TypeAlias

from .core.exceptions import (
    BusError,
    BusTypeError,
    EventError,
    HandlerError,
    NonValidEvent,
    TypesInconsistency,
    UnknownEventDataType,
    UnknownEventType,
    UnknownExitType,
    UnknownSearchType,
    UnknownSubscribeType,
    UnknownUniqType,
    WaitTimeoutError,
)
from .core.warnings import (
    BusWarning,
    EventWarning,
    HandlerWarning,
    NonValidEventWarning,
    PuttingFailedWarning,
    QueueFullWarning,
    QueueWarning,
    SubscribeTypeWarning,
    UnpredictableBusWarning,
    WaitTimeoutWarning,
)

DEBUG_MODE = threading.Event()
"""Флаг отладки"""
STACKLEVEL = 3
"""Уровень стека для предупреждений"""
EVENTSTACKLEVEL = 4
"""Уровень стека для дебаг-данных события"""


# Классы и типы:
class EasyCounter:
    """Потокобезопасный счетчик, возвращает значение, увеличенное (уменьшенное) на delta или текущее значение."""

    def __init__(self, initial: int = 0) -> None:
        self._value = initial
        self._lock = threading.Lock()

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


def check_flat(v: Dict[str, Any]) -> Dict[str, Any]:
    """Проверка плоскости словаря.

    Args:
        v (Dict[str, Any]): Словарь.

    Raises:
        ValueError: Если словарь не плоский.

    Returns:
        Dict[str, Any]: Плоский словарь.
    """
    for key, value in v.items():
        if isinstance(value, (dict, list, set, tuple)):
            raise ValueError(
                f"Value for key '{key}' must be a scalar, not {type(value).__name__}"
            )
    return v


class FlatDict(Dict[str, Any]):
    """Тип плоский словарь"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        check_flat(self)

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: GetCoreSchemaHandler
    ) -> core_schema.CoreSchema:
        # Мы просто берем стандартную схему словаря и вешаем на нее валидатор
        return core_schema.no_info_after_validator_function(
            check_flat, handler(dict[str, Any])
        )


class SubscriptionStorage(TypedDict):
    lists: dict[str | int, list[Callable]]
    id_sets: dict[str | int, set[int]]


# Enum-классы списков возможных значений
class UniqType(str, Enum):
    """Виды логики уникальности"""

    NONE = "NONE"
    WAIT = "WAIT"
    REPLACE = "REPLACE"


class ExitType(str, Enum):
    """Виды логики выхода из ожидания"""

    REJECT = "REJECT"
    PUT = "PUT"


class SearchType(str, Enum):
    """Виды логики поиска события в очереди."""

    NAME = "NAME"
    """Поиск по совпадению имени."""
    ID = "ID"
    """Поиск по совпадению имени и метаданных (полное совпадение)."""
    NUMBER = "NUMBER"
    """Поиск единичного события по уникальному порядковому номеру."""


class SubscribeType(str, Enum):
    """Виды логики подписки."""

    NUMBER = "NUMBER"
    """Подписка на уникальный порядковый номер."""
    ID = "ID"
    """Подписка на имя и метаданные (полное совпадение)."""
    NAME = "NAME"
    """Подписка на имя события (сигнал)."""


warnings.filterwarnings("ignore", category=NonValidEventWarning)


# События
class EventParent:
    _counter_iterator = itertools.count()

    @staticmethod
    def get_id(e_type: str, e_name: str, e_meta: dict) -> int:
        """Рассчитывает id события"""
        meta_part = str(sorted(e_meta.items())) if e_meta else ""
        raw_key = f"{e_type}:{e_name}:{meta_part}"
        hash_digest = hashlib.md5(raw_key.encode("utf-8")).hexdigest()

        return int(hash_digest, 16) & ((1 << 128) - 1)


@total_ordering
class Event(EventParent, BaseModel):
    """Контейнер события. Содержит идентификационные данные, полезную нагрузку, приоритет, настройки поведения в очереди. Сравнимый (по приоритету, возрасту)."""

    # Настройка модели
    model_config = ConfigDict(
        arbitrary_types_allowed=True,  # Позволит хранить любые объекты в args/kwargs
        use_enum_values=True,  # При сериализации Enum превратится в строку
    )

    # Паспорт события
    _event_id: int = PrivateAttr(default=-1)
    _event_number: int = PrivateAttr(
        default_factory=lambda: next(EventParent._counter_iterator)
    )
    _event_type: str = PrivateAttr(default="Event")

    _valid_flag: bool = PrivateAttr(default=True)

    # Полезная нагрузка
    name: str
    meta: FlatDict = Field(default_factory=FlatDict, exclude=True)
    args: tuple[Any,] = Field(default_factory=tuple[Any,])
    kwargs: dict[str, Any] = Field(default_factory=dict[str, Any])

    # Поведение в очереди
    priority: int = Field(default=100)
    priority_counter: int = Field(default=-1)

    uniq_type: UniqType = Field(default=UniqType.NONE)
    uniq_counter: int = Field(default=1)
    wait_timeout: float | None = Field(default=10)
    wait_timeout_exit: ExitType = Field(default=ExitType.REJECT)

    put_block: bool = Field(default=True)
    put_timeout: float | None = Field(default=None)

    def model_post_init(self, context: Any) -> None:
        super().model_post_init(context)
        self.refresh()

    def refresh(self) -> None:
        self._event_type = self.__class__.__name__
        self.priority_counter = self._event_number

        self._event_id = self.get_id(self._event_type, self.name, self.meta)

        if self.meta:
            common_keys = self.kwargs.keys() & self.meta.keys()
            if common_keys:
                warnings.warn(
                    f"In event (type={self._event_type}, name={self.name}, meta={self.meta}) meta and kwargs have identic attributes! The kwargs attributes were choosen.",
                    EventWarning,
                    stacklevel=STACKLEVEL,
                )

            to_update = self.meta.keys() - self.kwargs.keys()

            for key in to_update:
                self.kwargs[key] = self.meta[key]

        # Отладочная информация, перед компиляцией необходимо заккоментировать
        if DEBUG_MODE.is_set():
            try:
                func_name = sys._getframe(EVENTSTACKLEVEL).f_code.co_name

                extra_meta = {"_func_name": func_name, "_signal_name": self.name}
                self.kwargs.update(extra_meta)
            except Exception:
                pass

    @property
    def id(self) -> int:
        return self._event_id

    @property
    def num(self) -> int:
        return self._event_number

    @property
    def type(self) -> str:
        return self._event_type

    @property
    def is_valid(self) -> bool:
        return self._valid_flag

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Event):
            return NotImplemented

        return (self.priority, self.priority_counter) == (
            other.priority,
            other.priority_counter,
        )

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, Event):
            return NotImplemented

        return (self.priority, self.priority_counter) < (
            other.priority,
            other.priority_counter,
        )

    def make_valid(self) -> None:
        """Установка валидности события"""
        self._valid_flag = True

    def make_nonvalid(self) -> None:
        """Установка невалидности события"""
        self._valid_flag = False

    def __repr_args__(self) -> Iterable[tuple[Optional[str], Any]]:
        """Переопределение repr для добавления туда приватных атрибутов"""
        args = dict(super().__repr_args__())
        private_args = {
            name: getattr(self, name)
            for name in self.__private_attributes__
            if hasattr(self, name)
        }

        args.update(private_args)
        return args.items()

    def __init_subclass__(
        cls, **kwargs
    ):  # TODO: Разрешить сложное наследование, добавить проверку на имя по умолчанию (опционально)
        """Проверка на переопределение типов в подклассах

        Raises:
            EventError: Если первый родитель подкласса - не **Event**
            EventError: Если переопределены типы

        Examples:
        Правильное объявление именнованного класса:

        >>> class MyRightEvent(Event):
        ...     name: str = Field(default="right")
        ...     meta: FlatDict = Field(default_factory=lambda: FlatDict(meta1=True, meta2="check"))
        >>> print(MyRightEvent)
        <class '...MyRightEvent'>

        Неправильное объявление:

        >>> try: # doctest: +ELLIPSIS
        ...     class MyWrongEvent(Event):
        ...         name: str = Field(default="wrong")
        ...         meta: dict = Field(default_factory=lambda: {"meta1": True, "meta2": "check"})
        ... except Exception as e:
        ...     print(f"Ошибка: {e}")
        Ошибка: ...Types changing is rejected!
        """
        super().__init_subclass__(**kwargs)
        parent = cls.__bases__[0]

        if (parent) is not Event:
            raise EventError(
                f"For class {cls.__name__} class Event must be the first parent!"
            )

        def get_base(t: Any) -> Any:
            all_types = (get_origin(t), get_args(t))
            return all_types if all_types[0] is not None else t

        try:
            parent_hints = get_type_hints(parent)
            child_hints = get_type_hints(cls)
        except Exception as e:
            warnings.warn(
                f"Could not verify types for {cls.__name__} due to: {e}",
                UnpredictableBusWarning,
            )
            return

        for _field, new_type in child_hints.items():
            if _field in parent_hints:
                old_type = parent_hints[_field]

                if get_base(new_type) != get_base(old_type):
                    raise EventError(
                        f"Field '{_field}' in class {cls.__name__} changes field type '{old_type.__name__}' `{old_type}` to '{new_type.__name__}' `{new_type}`. Types changing is rejected!"
                    )

    @staticmethod
    def get_default_data(
        event_class: Enum | type["Event"],  # FIXME: Перевести на любой Enum
    ) -> tuple[str, str, FlatDict]:
        """Возвращает информацию о значимых для идентификации событий данных - тип (имя класса), имя события (сигнал), метаданные. На вход принимает класс типового или именнованного события, а не экземпляр. У переданного класса обязано быть задано имя события по умолчанию.

        Args:
            event_class (TyEv | type[Event]): Класс типового или именованного события.

        Raises:
            UnknownEventDataType: Если передан не класс, наследуемый от Event.
            EventError: Если у класса не определено имя по умолчанию.

        Returns:
            tuple[str, str, dict]: Кортеж тип-имя-метаданные.

        Examples:

            >>> print(Event.get_default_data(TyEv.START))
            ('START_EVENT', 'START', {})

            >>> print(issubclass(TyEv.START.value, Event))
            True

            >>> print(isinstance(TyEv.START.value, type))
            True

            >>> class MyEventClass(Event):
            ...     name: str = "my name"
            ...     meta: FlatDict = FlatDict(my=0)

            >>> print(issubclass(MyEventClass, Event))
            True

            >>> print(isinstance(MyEventClass, type))
            True

        """
        if isinstance(event_class, Enum):
            event_cls: type[Event] = event_class.value
        else:
            event_cls: type[Event] = event_class

        if not (isinstance(event_cls, type) and issubclass(event_cls, Event)):
            raise UnknownEventDataType(
                f"Expected Event subclass, got {type(event_cls)}"
            )

        else:
            event_type = event_cls.__name__

            name_field = event_cls.model_fields.get("name")
            if not name_field or name_field.default is PydanticUndefined:
                raise EventError(f"Class {event_type} must have a default 'name'.")

            name = name_field.default

            meta_field = event_cls.model_fields.get("meta")
            meta: FlatDict = FlatDict()
            if meta_field:
                if meta_field.default is not PydanticUndefined:
                    meta = copy.copy(meta_field.default)
                elif meta_field.default_factory is not None:
                    meta = meta_field.default_factory()  # type: ignore

            return (event_type, name, meta)


# Типовые события
class BUS_ERROR_EVENT(Event):
    name: str = Field(default="ERROR")
    meta: FlatDict = Field(
        default_factory=lambda: FlatDict(source="bus", type="handler")
    )
    kwargs: dict[str, Any] = Field(
        default_factory=lambda: {"txt": "Ошибка через шину. Ошибка в обработчике."}
    )
    priority: int = Field(default=5)
    put_block: bool = Field(default=False)


class LAUNCH_EVENT(Event):
    name: str = Field(default="LAUNCH")
    kwargs: dict[str, Any] = Field(default_factory=lambda: {"txt": "Запуск."})
    priority: int = Field(default=25)


class START_EVENT(Event):
    name: str = Field(default="START")
    kwargs: dict[str, Any] = Field(default_factory=lambda: {"txt": "Старт."})
    priority: int = Field(default=25)


class PAUSE_EVENT(Event):
    name: str = Field(default="PAUSE")
    kwargs: dict[str, Any] = Field(default_factory=lambda: {"txt": "Пауза."})
    priority: int = Field(default=25)


class STOP_EVENT(Event):
    name: str = Field(default="STOP")
    kwargs: dict[str, Any] = Field(default_factory=lambda: {"txt": "Остановка."})
    priority: int = Field(default=25)


class END_EVENT(Event):
    name: str = Field(default="END")
    kwargs: dict[str, Any] = Field(default_factory=lambda: {"txt": "Конец."})
    priority: int = Field(default=200)


class CANCEL_EVENT(Event):
    name: str = Field(default="CANCEL")
    kwargs: dict[str, Any] = Field(default_factory=lambda: {"txt": "Отмена."})
    priority: int = Field(default=25)


class UPDATE_EVENT(Event):
    name: str = Field(default="UPDATE")
    kwargs: dict[str, Any] = Field(default_factory=lambda: {"txt": "Обновление."})
    uniq_type: UniqType = Field(default=UniqType.REPLACE)


class LOG_EVENT(Event):
    name: str = Field(default="LOG")
    kwargs: dict[str, Any] = Field(default_factory=lambda: {"txt": "Логирование."})


class ERROR_EVENT(Event):
    name: str = Field(default="ERROR")
    kwargs: dict[str, Any] = Field(default_factory=lambda: {"txt": "Логирование."})
    priority: int = Field(default=10)


# Enum-список типовых событий
class TyEv(Enum):
    BUS_ERROR = BUS_ERROR_EVENT
    LAUNCH = LAUNCH_EVENT
    START = START_EVENT
    PAUSE = PAUSE_EVENT
    STOP = STOP_EVENT
    CANCEL = CANCEL_EVENT
    END = END_EVENT
    UPDATE = UPDATE_EVENT
    LOG = LOG_EVENT
    ERROR = ERROR_EVENT

    def create(self, **overrides) -> Event:
        return self.value(**overrides)

    def __call__(self, **kwargs):
        return self.create(**kwargs)

    @property
    def event(self) -> type[Event]:
        return self.value


# Класс подписчиков (хэндлеров)
class Handler(BaseModel):
    """Хэндлер (подписчик). Специализированный контейнер для хранения подписчика. Рекомендуется к использованию для передачи в подписки. При вызове принимает любые аргументы, очищает для дальнейшего проброса. Допускается создание копий через .duble() для повторных вызовов в одной очереди.

    Args:
        func (Callable): Вызываемая функция.
        default_kwargs (dict): Значения по умолчанию для функции, перезаписываются значениями, пришедшими извне при вызове.
        force_kwargs (dict): Значения, подставляемые в функцию насильно, перезаписывают значения, пришедшие извне при вызове.

    Raises:
        EventError: Если при создании подкласса переопределены типы полей.

    Examples:
        Функция для вызова:

        >>> def print_func(label: str, txt: str, num: int):
        ...     print(f"{label}:{txt}:{num}")

        Создание хэндлера:

        >>> handler = Handler(func=print_func, default_kwargs={"txt": "_paste text_"}, force_kwargs={"num": 1000})

        Вызов хэндлера (отсутствует аргумент **txt**, подставится из **default_kwargs**; аргумент **num** передан, но заменен из **force_kwargs**; присутствует лишний аргумент **unnessesary**, отбросится при вызове):

        >>> handler("MyLabel", num=-4, unnecessary="without error")
        MyLabel:_paste text_:1000
    """

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        use_enum_values=True,
    )

    _id: int = PrivateAttr(default=-1)

    func: Callable
    default_kwargs: dict = Field(default_factory=dict)
    force_kwargs: dict = Field(default_factory=dict)

    _has_kwargs: bool = PrivateAttr(default=False)
    _mask_params: set[str] = PrivateAttr(default_factory=set[str])

    _is_async: bool = PrivateAttr(default=False)

    def model_post_init(self, context: Any) -> None:
        super().model_post_init(context)
        self.refresh()

    def refresh(self):
        self._id = id(self)

        self._has_kwargs = False
        exclude_params = set()

        params = dict(inspect.signature(self.func).parameters)
        for name, param in params.items():
            if not self._has_kwargs:
                self._has_kwargs = param.kind == inspect.Parameter.VAR_KEYWORD

            if (
                param.kind == inspect.Parameter.VAR_KEYWORD
                or param.kind == inspect.Parameter.VAR_POSITIONAL
                or param.kind == inspect.Parameter.POSITIONAL_ONLY
            ):
                exclude_params.add(name)

        if not self._has_kwargs:
            self._mask_params = params.keys() - exclude_params

        self._is_async = inspect.iscoroutinefunction(self.func)

    def __call__(self, *args, **kwargs) -> Any:
        if self._has_kwargs:
            target_kwargs = {**self.default_kwargs, **kwargs, **self.force_kwargs}
            return self._run(*args, **target_kwargs)

        matching_keys = kwargs.keys() & self._mask_params
        matching_kwargs = {k: v for k, v in kwargs.items() if k in matching_keys}

        target_kwargs = {**self.default_kwargs, **matching_kwargs, **self.force_kwargs}

        return self._run(*args, **target_kwargs)

    def _run(self, *args, **kwargs) -> Any:
        if self._is_async:
            return self._run_async(*args, **kwargs)
        return self.func(*args, **kwargs)

    async def _run_async(self, *args, **kwargs) -> Any:
        return await self.func(*args, **kwargs)

    @property
    def id(self) -> int:
        return self._id

    def __init_subclass__(cls, **kwargs):
        """Проверка на переопределение типов в подклассах

        Raises:
            EventError: Если первый родитель подкласса - не **Event**
            EventError: Если переопределены типы

        Examples:
        >>> def default_f():
        ...     pass

        Правильное объявление именнованного класса:

        >>> class MyRightHandler(Handler):
        ...     func: Callable = Field(default=default_f)
        >>> print(MyRightHandler) # doctest: +ELLIPSIS
        <class '...MyRightHandler'>

        Неправильное объявление:

        >>> try: # doctest: +ELLIPSIS
        ...     class MyWrongHandler(Handler):
        ...         func: object = Field(default=default_f)
        ... except Exception as e:
        ...     print(f"Ошибка: {e}")
        Ошибка: ...Types changing is rejected!
        """
        super().__init_subclass__(**kwargs)
        parent = cls.__bases__[0]

        if (parent) is not Handler:
            raise EventError(
                f"For class {cls.__name__} class Handler must be the first parent!"
            )

        def get_base(t: Any) -> Any:
            all_types = (get_origin(t), get_args(t))
            return all_types if all_types[0] is not None else t

        try:
            parent_hints = get_type_hints(parent)
            child_hints = get_type_hints(cls)
        except Exception as e:
            warnings.warn(
                f"Could not verify types for {cls.__name__} due to: {e}",
                UnpredictableBusWarning,
            )
            return

        for _field, new_type in child_hints.items():
            if _field in parent_hints:
                old_type = parent_hints[_field]

                if get_base(new_type) != get_base(old_type):
                    raise HandlerError(
                        f"Field '{_field}' in class {cls.__name__} changes field type '{old_type.__name__}' `{old_type}` to '{new_type.__name__}' `{new_type}`. Types changing is rejected!"
                    )

    def duble(self, **update_kwargs) -> Self:
        """Создает дубликат объекта (с возможностью заменить значения полей по умолчанию). Является новым объектом, что позволяет повторять хэндлеры в очереди.

        Returns:
            Handler: Хэндлер (подписчик).

        Examples:

        >>> def first_func(txt: str):
        ...     print(f"Hello {txt}!")
        >>> def last_func(bye: str):
        ...     print(f"Goodbye {bye}!")

        >>> hello = Handler(func=first_func, default_kwargs={"txt": "World"})
        >>> hello()
        Hello World!

        >>> hello.duble()()
        Hello World!

        >>> hello.duble(default_kwargs={"txt": "Moon"})()
        Hello Moon!

        >>> goodbye = Handler(func=first_func, default_kwargs={"bye": "World"})
        >>> bye_set = {goodbye.id, goodbye.duble(default_kwargs={"bye": "Moon"}).id}
        >>> print(bye_set) # doctest: +ELLIPSIS
        {..., ...}

        >>> goodbye.duble() is goodbye
        False

        >>> goodbye.duble() == goodbye
        False

        >>> goodbye.duble(default_kwargs={"bye": "Moon"}).id in bye_set
        False
        """
        duble_handler = self.model_copy(deep=False, update=update_kwargs)
        duble_handler.default_kwargs = dict(
            self.smartcopy(duble_handler.default_kwargs)
        )

        duble_handler.refresh()

        return duble_handler

    @staticmethod
    def smartcopy(data: dict | list | Any) -> dict | list | Any:
        """Рекурсивное глубокое копирование всех non-callable объектов. Для некопируемых и callable объектов вместо копии возвращает оригинал. Предпочтительно использовать для копирования сложных списков, словарей, в которых могут попадаться ссылки на функции.

        Args:
            data (dict | list | Any): Данные для копирования.

        Raises:
            TypeError: Если переданные данные содержат объекты, не способные к копированию.

        Returns:
            dict | list | Any: Скопированные данные.
        """
        if isinstance(data, dict):
            return {k: Handler.smartcopy(v) for k, v in data.items()}

        elif isinstance(data, list):
            return [Handler.smartcopy(v) for v in data]

        elif callable(data):
            return data

        else:
            try:
                return copy.deepcopy(data)
            except TypeError:
                return data

    @staticmethod
    def get_handler_name(h: Callable | "Handler") -> str:
        while isinstance(h, Handler) or (hasattr(h, "func") and hasattr(h, "args")):
            h = getattr(h, "func")

        if hasattr(h, "__self__"):
            class_name = getattr(h, "__self__").__class__.__name__
            return f"{class_name}.{h.__name__}"

        if hasattr(h, "__class__") and not isinstance(
            h, (type, types.FunctionType, types.BuiltinFunctionType)
        ):
            return f"{h.__class__.__name__}.__call__"

        if getattr(h, "__name__", "") == "<lambda>":
            return "<lambda>"

        return getattr(h, "__name__", str(h))


# Ядро (очередь)
class UniquePriorityQueue:
    """Специальная очередь событий, приоритеризирована и имеет проверку на уникальность событий"""

    @dataclass
    class Inspection:
        """Самоинспекция"""

        wait_warnings_amount: EasyCounter = field(default_factory=EasyCounter)
        wait_errors_amount: EasyCounter = field(default_factory=EasyCounter)
        nonvalid_events_gotten: EasyCounter = field(default_factory=EasyCounter)
        putting_failed: EasyCounter = field(default_factory=EasyCounter)
        events_cleaned: EasyCounter = field(default_factory=EasyCounter)

        def snapshot(self) -> dict:
            """Превращает в словарь."""
            return {
                k: int(v) if isinstance(v, EasyCounter) else v
                for k, v in self.__dict__.items()
            }

    def __init__(self, maxsize=0) -> None:
        self.maxsize = maxsize

        self.queue = queue.PriorityQueue(maxsize=self.maxsize)
        self._ids: dict[int, dict[int, Event]] = {}
        """Сопровождающий словарь, сгруппирован по id, формат: [event.id: {event.num: event}]"""

        self._condition = threading.Condition()
        """Замок состояния"""

        self._inspection = self.Inspection()

    # Взаимодействия с очередью
    def put(self, event: Event) -> None:
        with self._condition:
            if event.uniq_type == UniqType.NONE:
                self._bare_put(event)

            elif event.uniq_type == UniqType.WAIT:
                self._waiting_put(event)

            elif event.uniq_type == UniqType.REPLACE:
                self._replacing_put(event)

            else:
                raise UnknownUniqType("Unknown unique logic type received!")

    def _bare_put(self, event: Event) -> None:
        self._add_to_queue(event)

    def _waiting_put(self, event: Event) -> None:
        with self._condition:
            success = self._condition.wait_for(
                lambda: len(self._ids.get(event.id, {})) < event.uniq_counter,
                timeout=event.wait_timeout,
            )

            if not success:
                if event.wait_timeout_exit == ExitType.REJECT:
                    self._inspection.wait_errors_amount()
                    raise WaitTimeoutError(
                        f"The WAIT logic event (name='{event.name}', id={event.id}, num={event.num}) was rejected by timeout!"
                    )
                elif event.wait_timeout_exit == ExitType.PUT:
                    self._inspection.wait_warnings_amount()
                    warnings.warn(
                        f"Event(name='{event.name}', id={event.id}, num={event.num}) WAIT timeout exceeded. Forcing PUT into queue. Total WAIT timeout warnings amount: {int(self._inspection.wait_warnings_amount)}",
                        WaitTimeoutWarning,
                        stacklevel=STACKLEVEL,
                    )
                else:
                    raise UnknownExitType("Unknown exit logic type received!")

        self._add_to_queue(event)

    def _replacing_put(self, event: Event) -> None:
        with self._condition:
            _events_list = sorted(
                (e for e in self._ids.get(event.id, {}).values() if e.is_valid)
            )

            if _events_list:
                idx = min(event.uniq_counter - 1, len(_events_list) - 1)

                _old_event = _events_list[idx]

                _old_event.make_nonvalid()

                event.priority_counter = _old_event.priority_counter

        self._add_to_queue(event)

    def _add_to_queue(self, event: Event) -> None:
        with self._condition:
            id_group = self._ids.setdefault(event.id, {})
            id_group[event.num] = event

        try:
            self.queue.put(event, block=event.put_block, timeout=event.put_timeout)
        except (queue.Full, Exception) as e:
            with self._condition:
                self._remove_from_sattelite(event)
                self._condition.notify_all()

                self._inspection.putting_failed()
                warnings.warn(
                    f"Event(name='{event.name}', id={event.id}, num={event.num}) did not added to the queue. Total failed puttings amount: {int(self._inspection.putting_failed)}. Error: {e}",
                    PuttingFailedWarning,
                    stacklevel=STACKLEVEL,
                )

                raise

        with self._condition:
            self._condition.notify_all()

    def get(self, block: bool = True, timeout: float | None = None) -> Event:
        while True:
            event: Event = self.queue.get(block=block, timeout=timeout)

            with self._condition:
                self._remove_from_sattelite(event)
                self._condition.notify_all()

                if not event.is_valid:
                    self._inspection.nonvalid_events_gotten()
                    if DEBUG_MODE.is_set():
                        warnings.warn(
                            f"Event(name='{event.name}', id={event.id}, num={event.num}) was gotten as nonvalid. Total nonvalid events amount: {int(self._inspection.nonvalid_events_gotten)}",
                            NonValidEventWarning,
                            stacklevel=STACKLEVEL,
                        )

                    continue

                return event

    def _remove_from_sattelite(self, event: Event) -> None:
        id_group = self._ids.get(event.id)
        if id_group:
            id_group.pop(event.num, None)
            if not id_group:
                self._ids.pop(event.id)

    # Работа с обособленными событиями
    def search_events(
        self,
        search_type: SearchType,
        event_name: str = "",
        event_meta: dict | None = None,
        event_type: TyEv | type[Event] = Event,  # FIXME: Перевести на любой Enum
        event_num: int = -1,
    ) -> list[Event]:
        """Ищет событие (события) в очереди. Возвращает список найденных событий в порядке приоритета или пустой список, если событий не найдено.

        Args:
            search_type (SearchType): Вид поиска.
            event_name (str, optional): Имя события. Defaults to "".
            event_meta (dict, optional): Метаданные события (полное совпадение). Defaults to None.
            event_type (TyEv | type[Event], optional): Класс типа типового или нетипового события. Defaults to Event.
            event_num (int, optional): Порядковый номер события. Defaults to -1.

        Raises:
            UnknownSearchType: Если передан неизвестный вид поиска.
            UnknownEventDataType: Если передан неизвестный класс вместо события.
            EventError: Если у именнованного события не определено имя.

        Returns:
            list[Event]: Список объектов событий, отсортированных по приоритету.

        Notes:
            - Поиск по `NAME` требует *обязательной* передачи **имени**.

            -> Возвращает *все* события *любых типов* с переданным **именем**.

            - Поиск по `ID` требует передачи **типа** (default=`Event`) и **метаданных** (default=`None` -> `{}`) события. Если для типа события установлено значение имени *(в случае типовых событий или заранее созданных именованных типов)*, не требует, *но допускает*, передачу **имени**, в ином случае требует *обязательной* передачи.

            -> Возвращает события, *полностью совпадающие* по **имени**, **метаданным** и **типу**.

            - Поиск по `NUMBER` требует *обязательной* передачи порядкового **номера**.

            -> В стандартной ситуации возвращает список из *одного* события.

        Examples:
            Подготовка: Создаем очередь и добавляем данные
            >>> class SLEEP(Event):
            ...     name: str = "SLEEP"

            >>> _queue = UniquePriorityQueue()
            >>> e = Event(name="click", priority=5)
            >>> _queue.put(Event(name="click", priority=10))
            >>> _queue.put(e)
            >>> _queue.put(SLEEP())
            >>> _queue.put(TyEv.CANCEL(meta={"window": "main"}, kwargs={'button': 1}))
            >>> _queue.put(TyEv.CANCEL(name="CANCEL", meta={"window": "main"}, kwargs={'button': 2}))

            Для поиска по имени:
            >>> _queue.search_events(SearchType.NAME, event_name="click") # doctest: +ELLIPSIS
            [Event(name='click', ...), Event(name='click', ...)]

            Для поиска по id (тип, имя, метаданные):
            >>> _queue.search_events(SearchType.ID, event_type=SLEEP, event_name="SLEEP") # doctest: +ELLIPSIS
            [SLEEP(name='SLEEP', ...)]

            >>> _queue.search_events(SearchType.ID, event_type=TyEv.CANCEL, event_name="CANCEL", event_meta={"window": "main"}) # doctest: +ELLIPSIS
            [...]
            >>> _queue.search_events(SearchType.ID, event_type=TyEv.CANCEL, event_meta={"window": "main"}) # doctest: +ELLIPSIS
            [CANCEL_EVENT(name='CANCEL', meta={'window': 'main'}, ...kwargs={'button': 1...}, ...), CANCEL_EVENT(name='CANCEL', meta={'window': 'main'}, ...kwargs={'button': 2...}, ...)]

            Для поиска по номеру:
            >>> found = _queue.search_events(SearchType.NUMBER, event_num=e.num)

            Проверка, что найдено 1 событие
            >>> len(found)
            1

            Проверка совпадения номеров
            >>> found[0]._event_number == e.num
            True
        """
        searched_events: list[Event] = []

        with self._condition:
            if search_type == SearchType.NAME:
                for id_group in self._ids.values():
                    for event in id_group.values():
                        if event.name == event_name:
                            searched_events.append(event)

            elif search_type == SearchType.ID:
                default_data = Event.get_default_data(event_type)
                e_type, default_name, default_meta = default_data

                name = event_name if event_name else default_name
                meta = event_meta if event_meta is not None else default_meta

                event_id = Event.get_id(e_type, name, meta)

                for event in self._ids.get(event_id, {}).values():
                    searched_events.append(event)

            elif search_type == SearchType.NUMBER:
                for id_group in self._ids.values():
                    if event_num in id_group:
                        searched_events.append(id_group[event_num])
                        break

            else:
                raise UnknownSearchType("Unknown search logic type received!")

            if not searched_events:
                return []

            return sorted(searched_events)

    def replace_event(self, new_event: Event, old_event: Event) -> None:
        """Заменяет одно событие в очереди другим, ставя новое событие хронологически на то же место. Все поля, включая название сигнала, id, приоритет и метаданные, обновляются.

        Args:
            new_event (Event): Замещяющее событие.
            old_event (Event): Замещаемое событие.

        Notes:
            Событие кладется в очередь через метод .put(), что позволяет использовать для него любую логику уникальности.
        """
        with self._condition:
            old_event.make_nonvalid()

            new_event.priority_counter = old_event.priority_counter

        self.put(new_event)

    def devalid_event(self, event: Event) -> None:
        """Девалидизация заданного события, используется только для ручной девалидизации.

        Args:
            event (Event): Событие.
        """
        with self._condition:
            event.make_nonvalid()

    def valid_event(self, event: Event) -> None:
        """Ревалидизация заданного события, используется только для ручной ревалидизации.

        Args:
            event (Event): Событие.
        """
        with self._condition:
            event.make_valid()

    def clean_queue(self) -> None:
        """Очистка очереди."""
        with self._condition:
            while True:
                try:
                    self.queue.get_nowait()
                    self.queue.task_done()

                    self._inspection.events_cleaned()

                except queue.Empty:
                    break

            self._ids.clear()

            self._condition.notify_all()

    # Счетчики
    def task_done(self) -> None:
        self.queue.task_done()

    def join(self) -> None:
        self.queue.join()

    # Самоинспекция
    def qsize(self) -> int:
        with self._condition:
            return self.queue.qsize()

    def info(self) -> dict:
        """Информация о текущем состоянии очереди. Возвращает отчет, содержащий информацию о размере очереди, количестве id групп, {количестве полученных ошибок и предупреждений}, содержании словаря-спутика {id1: {количество событий, тип, имя, метаданные, [порядковые номера]}, id2:...}.

        Returns:
            dict: Отчет о текущем состоянии.

        Examples:
            Подготовка: Создаем очередь и добавляем данные
            >>> _queue = UniquePriorityQueue(10)
            >>> _queue.put(Event(name="click", priority=10))
            >>> _queue.put(TyEv.CANCEL(meta={"window": "main"}, kwargs={'button': 1}))
            >>> e = TyEv.CANCEL(name="CANCEL", meta={"window": "main"}, kwargs={'button': 2})
            >>> _queue.put(e)
            >>> e.make_nonvalid()

            Получение отчета:
            >>> _queue.info() # doctest: +ELLIPSIS
            {'qsize': 3, 'maxsize': 10, 'ids_amount': 2, 'inspection': {'wait_warnings_amount': ..., 'wait_errors_amount': ..., 'nonvalid_events_gotten': ..., 'putting_failed': ...}, 'satellite': {339514816366116273679071597463465878107: {'events_amount': 1, 'type': 'Event', 'name': 'click', 'meta': {}, 'nums': [(..., True)]}, 315106522019672546056486932092520271199: {'events_amount': 2, 'type': 'CANCEL_EVENT', 'name': 'CANCEL', 'meta': {'window': 'main'}, 'nums': [(..., True), (..., False)]}}}
        """
        with self._condition:
            report: dict = {
                "qsize": self.qsize(),
                "maxsize": self.maxsize,
                "ids_amount": len(self._ids),
                "inspection": self._inspection.snapshot(),
            }

            report["satellite"] = {}

            for event_id, id_group in self._ids.items():
                nums_amount = len(id_group)

                data_type = ""
                data_name = ""
                data_meta = {}
                nums = []

                if nums_amount:
                    first_event = next(iter(id_group.values()))

                    if first_event:
                        data_type = first_event.type
                        data_name = first_event.name
                        data_meta = first_event.meta

                        nums = [(k, e.is_valid) for k, e in id_group.items()]

                report["satellite"][event_id] = {
                    "events_amount": nums_amount,
                    "type": data_type,
                    "name": data_name,
                    "meta": data_meta.copy(),
                    "nums": nums,
                }

            return report


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
