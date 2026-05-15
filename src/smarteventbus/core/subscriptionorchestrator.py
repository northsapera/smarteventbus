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

"""An subscriber dispatcher focused on thread safety."""

import threading
import warnings
from enum import Enum
from typing import Callable, TypedDict

from .config import STACKLEVEL
from .custexceptions import (
    TypesInconsistency,
    UnknownEventDataType,
    UnknownSubscribeType,
)
from .custwarnings import SubscribeTypeWarning
from .eventclasses import Event, TyEv
from .handlerclasses import Handler
from .logictypes import SubscribeType


class SubscriptionStorage(TypedDict):
    lists: dict[str | int, list[Callable]]
    sets: dict[str | int, set[Callable]]


class SubscriptionOrchestrator:
    def __init__(self) -> None:
        self._lock = threading.Lock()

        self._subscribers: dict[SubscribeType, SubscriptionStorage] = {
            SubscribeType.NUMBER: {"lists": {}, "sets": {}},
            SubscribeType.ID: {"lists": {}, "sets": {}},
            SubscribeType.NAME: {"lists": {}, "sets": {}},
        }

    def _create_record(
        self, sub_type: SubscribeType, signal: str | int, handler: Handler | Callable
    ) -> None:
        subscribers_group = self._subscribers[sub_type]

        handlers_list = subscribers_group["lists"].setdefault(signal, [])
        handlers_set = subscribers_group["sets"].setdefault(signal, set())

        if handler not in handlers_set:
            handlers_list.append(handler)
            handlers_set.add(handler)

    def subscribe(
        self,
        event_data: str
        | Event
        | TyEv
        | Enum
        | type[Event]
        | int,  # NOTE: Перевести на любой Enum (сделано?)
        handlers: Callable | Handler | list[Callable | Handler],
        subscribe_type: SubscribeType = SubscribeType.NAME,
    ) -> None:
        with self._lock:
            signal = None
            if callable(handlers) or isinstance(handlers, Handler):
                handlers_iterable = [handlers]
            elif isinstance(handlers, (list, tuple, set)):
                handlers_iterable = handlers
            else:
                raise TypesInconsistency(
                    "Handlers must be a callable, a Handler instance, or a collection of them!"
                )

            if isinstance(event_data, (str, int)):
                signal = self._bare_event_data(event_data, subscribe_type)

            elif isinstance(event_data, Event):
                signal = self._sample_event_data(event_data, subscribe_type)

            elif isinstance(event_data, (Enum, TyEv, type)):
                signal = self._class_event_data(event_data, subscribe_type)

            else:
                raise UnknownEventDataType("Unknown event data type received!")

            if signal is None:
                raise TypesInconsistency(
                    f"Event data {event_data} must comply with the SubscribeType. logic!"
                )

            for handler in handlers_iterable:
                if not (callable(handler) or isinstance(handler, Handler)):
                    raise TypesInconsistency(
                        f"Each handler must be callable! Handler '{handler}' is not."
                    )

                self._create_record(subscribe_type, signal, handler)

    # region get signal logic types
    def _bare_event_data(
        self, event_data: str | int, subscribe_type: SubscribeType
    ) -> str | int:
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

        return signal

    def _sample_event_data(
        self, event_data: Event, subscribe_type: SubscribeType
    ) -> str | int:
        try:
            mapping = {
                SubscribeType.NAME: event_data.name,
                SubscribeType.ID: event_data.id,
                SubscribeType.NUMBER: event_data.num,
            }
            signal = mapping[subscribe_type]
        except KeyError:
            raise UnknownSubscribeType("Unknown subscription type received!")

        return signal

    def _class_event_data(
        self, event_data: type[Event] | TyEv | Enum, subscribe_type: SubscribeType
    ) -> str | int:
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

        return signal

    # endregion get signal logic types

    def get_handlers_snapshot(self, event: Event) -> list[Callable]:
        with self._lock:
            snapshot = []

            signals = {
                SubscribeType.NUMBER: event.num,
                SubscribeType.ID: event.id,
                SubscribeType.NAME: event.name,
            }

            for sub_type, signal in signals.items():
                handlers = self._subscribers[sub_type]["lists"].get(signal)
                if handlers:
                    snapshot.extend(handlers)

            return snapshot

    @property
    def info(self) -> dict:
        with self._lock:
            subscribers = {
                t.value: {
                    "lists": {
                        i: [Handler.get_handler_name(h) for h in h_group]
                        for i, h_group in t_group["lists"].items()
                    },
                    "sets": {
                        i: [Handler.get_handler_name(h) for h in h_group]
                        for i, h_group in t_group["sets"].items()
                    },
                }
                for t, t_group in self._subscribers.items()
            }

            return subscribers
