from abc import abstractmethod
from dataclasses import dataclass
from typing import Iterable

from datahub.ingestion.api.source import Source


# See https://github.com/python/mypy/issues/5374 for why we suppress this mypy error.
@dataclass  # type: ignore[misc]
class SampleableSource(Source):
    @abstractmethod
    def sample(self, schema_name: str) -> Iterable[str]:
        pass
