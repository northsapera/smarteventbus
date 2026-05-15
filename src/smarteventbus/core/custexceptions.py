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

"""Custom exceptions for Smart Event Bus."""


# Именованные ошибки
class BusError(Exception):
    pass


class WaitTimeoutError(BusError):
    pass


class BusTypeError(BusError):
    pass


class UnknownUniqType(BusTypeError):
    pass


class UnknownExitType(BusTypeError):
    pass


class UnknownSearchType(BusTypeError):
    pass


class UnknownSubscribeType(BusTypeError):
    pass


class UnknownEventDataType(BusTypeError):
    pass


class UnknownEventType(BusTypeError):
    pass


class TypesInconsistency(BusTypeError):
    pass


class EventError(BusError):
    pass


class NonValidEvent(EventError):
    pass


class HandlerError(BusError):
    pass
