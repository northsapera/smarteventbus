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

"""Logic types: Enum lists."""

from enum import Enum, IntEnum, StrEnum


# Enum-классы списков возможных значений
class UniqType(StrEnum):
    """Виды логики уникальности"""

    NONE = "NONE"
    WAIT = "WAIT"
    REPLACE = "REPLACE"


class ExitType(StrEnum):
    """Виды логики выхода из ожидания"""

    REJECT = "REJECT"
    PUT = "PUT"


class SearchType(StrEnum):
    """Виды логики поиска события в очереди."""

    NAME = "NAME"
    """Поиск по совпадению имени."""
    ID = "ID"
    """Поиск по совпадению имени и метаданных (полное совпадение)."""
    NUMBER = "NUMBER"
    """Поиск единичного события по уникальному порядковому номеру."""


class PubType(StrEnum):
    """Виды логики публикации событий"""

    NONE = "NONE"
    """Еще не было в череди"""
    PUBLISH = "PUBLISH"
    """Чистая отправка в шину"""
    CALL = "CALL"
    """Ожидание результата"""
    FORWARD = "FORWARD"
    """Проброс функции"""
    PUBBACK = "PUBBACK"
    """Обратная публикация события"""


class SubscribeType(StrEnum):
    """Виды логики подписки."""

    NUMBER = "NUMBER"
    """Подписка на уникальный порядковый номер."""
    ID = "ID"
    """Подписка на имя и метаданные (полное совпадение)."""
    NAME = "NAME"
    """Подписка на имя события (сигнал)."""


class ThreadType(StrEnum):
    """Виды пулов потоков"""

    POOL = "pool"
    """Общий поток"""
    DEDICATED = "dedicated"
    """Выделенный поток"""

    INTERNAL_PUBLICATIONS = "_publications"
    """Внутренний тип для выделения потоков под асинхронные неблокирующие методы публикации"""
    INTERNAL_CALL_MANAGERS = "_call_managers"
    """Внутренний тип для выделения потоков под call-менеджер"""
