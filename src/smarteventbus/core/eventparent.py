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

"""Event parent with counter and static methods."""

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
