# Именованные предупреждения
class BusWarning(UserWarning):
    pass


class UnpredictableBusWarning(BusWarning):
    pass


class SubscribeTypeWarning(BusWarning):
    pass


class QueueWarning(BusWarning):
    pass


class PuttingFailedWarning(QueueWarning):
    pass


class WaitTimeoutWarning(QueueWarning):
    pass


class QueueFullWarning(QueueWarning):
    pass


class EventWarning(BusWarning):
    pass


class NonValidEventWarning(EventWarning):
    pass


class HandlerWarning(BusWarning):
    pass
