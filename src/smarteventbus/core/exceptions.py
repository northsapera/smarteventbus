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
