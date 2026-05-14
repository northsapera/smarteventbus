import hashlib
import itertools


class EventParent:
    _counter_iterator = itertools.count()

    @staticmethod
    def get_id(e_type: str, e_name: str, e_meta: dict) -> int:
        """Рассчитывает id события"""
        meta_part = str(sorted(e_meta.items())) if e_meta else ""
        raw_key = f"{e_type}:{e_name}:{meta_part}"
        hash_digest = hashlib.md5(raw_key.encode("utf-8")).hexdigest()

        return int(hash_digest, 16) & ((1 << 128) - 1)
