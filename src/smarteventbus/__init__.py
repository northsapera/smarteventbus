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

"""
Smart Event Bus: A thread-safe event bus library for Python.
"""

__version__ = "0.8.1"
__author__ = "Matvey Grigoryev"

from .core.config import debug_mode
from .core.eventclasses import Event, TyEv
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
from .core.handlerclasses import Handler
from .core.logictypes import ExitType, SearchType, SubscribeType, UniqType
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
from .eventbus import BusNetwork, EventBus, bus
from .utils.flatten import FlatDict, check_flat

# Определяем, что будет доступно при "from smarteventbus import *"
__all__ = [
    # Main Components
    "EventBus",
    "BusNetwork",
    "bus",
    "Event",
    "Handler",
    "TyEv",
    "debug_mode",
    # Logic types
    "ExitType",
    "SearchType",
    "SubscribeType",
    "UniqType",
    # Utils
    "FlatDict",
    "check_flat",
    # Exceptions
    "BusError",
    "BusTypeError",
    "EventError",
    "HandlerError",
    "NonValidEvent",
    "TypesInconsistency",
    "UnknownEventDataType",
    "UnknownEventType",
    "UnknownExitType",
    "UnknownSearchType",
    "UnknownSubscribeType",
    "UnknownUniqType",
    "WaitTimeoutError",
    # Warnings
    "BusWarning",
    "EventWarning",
    "HandlerWarning",
    "NonValidEventWarning",
    "PuttingFailedWarning",
    "QueueFullWarning",
    "QueueWarning",
    "SubscribeTypeWarning",
    "UnpredictableBusWarning",
    "WaitTimeoutWarning",
]
