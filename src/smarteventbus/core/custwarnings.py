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

"""Custom warnings for Smart Event Bus."""


# Именованные предупреждения
class BusWarning(UserWarning):
    pass


class UnpredictableBusWarning(BusWarning):
    pass


class SubscribeTypeWarning(BusWarning):
    pass


class ExecutorWarning(BusWarning):
    pass


class QueueWarning(BusWarning):
    pass


class PuttingFailedWarning(QueueWarning):
    pass


class WaitTimeoutWarning(QueueWarning):
    pass


class QueueFullWarning(QueueWarning):
    pass


class QueueResetWarning(QueueWarning):
    pass


class EventWarning(BusWarning):
    pass


class NonValidEventWarning(EventWarning):
    pass


class PotentialLoopWarning(EventWarning):
    pass


class HandlerWarning(BusWarning):
    pass
