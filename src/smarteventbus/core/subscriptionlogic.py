from typing import Callable, TypedDict


class SubscriptionStorage(TypedDict):
    lists: dict[str | int, list[Callable]]
    id_sets: dict[str | int, set[int]]
