from threading import Event as ThreadEvent

DEBUG_MODE = ThreadEvent()
"""Флаг отладки"""
EVENTSTACKLEVEL = 4
"""Уровень стека для дебаг-данных события"""
STACKLEVEL = 3
"""Уровень стека для предупреждений"""
