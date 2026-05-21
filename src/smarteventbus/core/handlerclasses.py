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

"""Handler classes for Smart Event Bus."""

import copy
import inspect
import types
import warnings
from typing import (
    Any,
    Callable,
    Self,
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

from .custexceptions import HandlerError
from .custwarnings import UnpredictableBusWarning
from .logictypes import ThreadType


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

    strict_order: bool = Field(default=True)
    context: str = Field(default=ThreadType.POOL)

    allow_pubback: bool = Field(default=True)

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

        try:
            return self.func(*args, **kwargs)
        except TypeError as e:
            e.add_note(
                "If func got multiple values for argument, сheck for mixing positional and named arguments in handler calling."
            )
            raise

    async def _run_async(self, *args, **kwargs) -> Any:
        try:
            result = await self.func(*args, **kwargs)
            return result
        except TypeError as e:
            e.add_note(
                "If func got multiple values for argument, сheck for mixing positional and named arguments in handler calling."
            )
            raise

    @property
    def id(self) -> int:
        return self._id

    @property
    def is_async(self) -> bool:
        return self._is_async

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
            raise HandlerError(
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

    def __hash__(self) -> int:
        return hash(self._id)

    def __eq__(self, other) -> bool:
        if not isinstance(other, Handler):
            return False
        return self._id == other._id
