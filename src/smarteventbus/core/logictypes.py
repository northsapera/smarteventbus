from enum import Enum


# Enum-классы списков возможных значений
class UniqType(str, Enum):
    """Виды логики уникальности"""

    NONE = "NONE"
    WAIT = "WAIT"
    REPLACE = "REPLACE"


class ExitType(str, Enum):
    """Виды логики выхода из ожидания"""

    REJECT = "REJECT"
    PUT = "PUT"


class SearchType(str, Enum):
    """Виды логики поиска события в очереди."""

    NAME = "NAME"
    """Поиск по совпадению имени."""
    ID = "ID"
    """Поиск по совпадению имени и метаданных (полное совпадение)."""
    NUMBER = "NUMBER"
    """Поиск единичного события по уникальному порядковому номеру."""


class SubscribeType(str, Enum):
    """Виды логики подписки."""

    NUMBER = "NUMBER"
    """Подписка на уникальный порядковый номер."""
    ID = "ID"
    """Подписка на имя и метаданные (полное совпадение)."""
    NAME = "NAME"
    """Подписка на имя события (сигнал)."""
