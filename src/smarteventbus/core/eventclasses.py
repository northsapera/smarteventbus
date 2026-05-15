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

"""Event classes: base and typical."""

import copy
import sys
import warnings
from enum import Enum
from functools import total_ordering
from typing import (
    Any,
    Iterable,
    Optional,
    get_args,
    get_origin,
    get_type_hints,
)

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    PrivateAttr,
)
from pydantic_core import PydanticUndefined

from ..utils.flatten import FlatDict
from .config import EVENTSTACKLEVEL, STACKLEVEL, debug_mode
from .custexceptions import (
    EventError,
    UnknownEventDataType,
    UnknownExitType,
    UnknownUniqType,
)
from .custwarnings import EventWarning, UnpredictableBusWarning
from .eventparent import EventParent
from .logictypes import ExitType, UniqType


# События
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
    wait_timeout_exit: ExitType = Field(default=ExitType.REJECT)

    block: bool = Field(default=True)
    timeout: float | None = Field(default=None)

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
        if debug_mode.is_set():
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
        event_class: Enum | type["Event"],  # NOTE: Перевести на любой Enum (сделано?)
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
