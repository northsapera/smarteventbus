from typing import Any, Dict

from pydantic import GetCoreSchemaHandler
from pydantic_core import core_schema


def check_flat(v: Dict[str, Any]) -> Dict[str, Any]:
    """Проверка плоскости словаря.

    Args:
        v (Dict[str, Any]): Словарь.

    Raises:
        ValueError: Если словарь не плоский.

    Returns:
        Dict[str, Any]: Плоский словарь.
    """
    for key, value in v.items():
        if isinstance(value, (dict, list, set, tuple)):
            raise ValueError(
                f"Value for key '{key}' must be a scalar, not {type(value).__name__}"
            )
    return v


class FlatDict(Dict[str, Any]):
    """Тип плоский словарь"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        check_flat(self)

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: GetCoreSchemaHandler
    ) -> core_schema.CoreSchema:
        # Мы просто берем стандартную схему словаря и вешаем на нее валидатор
        return core_schema.no_info_after_validator_function(
            check_flat, handler(dict[str, Any])
        )
