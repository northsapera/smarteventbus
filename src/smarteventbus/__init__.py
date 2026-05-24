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

__version__ = "0.14.0"
__author__ = "Matvey Grigoryev"

from .core.config import debug_mode
from .core.custexceptions import (
    BusError,
    BusTypeError,
    CallTimeoutError,
    CannotEnd,
    EventError,
    ExecutorError,
    ExecutorInitError,
    HandlerError,
    NonValidEvent,
    PotentialLoop,
    QueueEmpty,
    QueueError,
    QueueFull,
    QueueReset,
    RePublishing,
    StopTimeoutError,
    TasksCounterError,
    ThreadingsError,
    TypesInconsistency,
    UnknownEventDataType,
    UnknownEventType,
    UnknownExecutorConfig,
    UnknownExitType,
    UnknownSearchType,
    UnknownSubscribeType,
    UnknownUniqType,
    UnReadToken,
    WaitTimeoutError,
)
from .core.custwarnings import (
    BusWarning,
    EventWarning,
    ExecutorWarning,
    HandlerWarning,
    NonValidEventWarning,
    PotentialLoopWarning,
    PuttingFailedWarning,
    QueueFullWarning,
    QueueResetWarning,
    QueueWarning,
    SubscribeTypeWarning,
    UnpredictableBusWarning,
    WaitTimeoutWarning,
)
from .core.eventclasses import Event, TyEv
from .core.handlerclasses import Handler, register
from .core.logictypes import (
    ExitType,
    PubType,
    SearchType,
    SubscribeType,
    ThreadType,
    UniqType,
)
from .eventbus import BusNetwork, EventBus, bus, subscribe_to
from .utils.flatten import FlatDict, check_flat

# Определяем, что будет доступно при "from smarteventbus import *"
__all__ = [
    # Main Components
    "EventBus",
    "BusNetwork",
    "bus",
    "Event",
    "Handler",
    "register",
    "subscribe_to",
    "TyEv",
    "debug_mode",
    # Logic types
    "ExitType",
    "PubType",
    "SearchType",
    "SubscribeType",
    "ThreadType",
    "UniqType",
    # Utils
    "FlatDict",
    "check_flat",
    # Exceptions
    "BusError",
    "BusTypeError",
    "CallTimeoutError",
    "CannotEnd",
    "EventError",
    "ExecutorError",
    "ExecutorInitError",
    "HandlerError",
    "NonValidEvent",
    "PotentialLoop",
    "QueueEmpty",
    "QueueError",
    "QueueFull",
    "QueueReset",
    "RePublishing",
    "StopTimeoutError",
    "TasksCounterError",
    "ThreadingsError",
    "TypesInconsistency",
    "UnknownEventDataType",
    "UnknownEventType",
    "UnknownExecutorConfig",
    "UnknownExitType",
    "UnknownSearchType",
    "UnknownSubscribeType",
    "UnReadToken",
    "UnknownUniqType",
    "WaitTimeoutError",
    # Warnings
    "BusWarning",
    "EventWarning",
    "ExecutorWarning",
    "HandlerWarning",
    "NonValidEventWarning",
    "PotentialLoopWarning",
    "PuttingFailedWarning",
    "QueueFullWarning",
    "QueueResetWarning",
    "QueueWarning",
    "SubscribeTypeWarning",
    "UnpredictableBusWarning",
    "WaitTimeoutWarning",
]
