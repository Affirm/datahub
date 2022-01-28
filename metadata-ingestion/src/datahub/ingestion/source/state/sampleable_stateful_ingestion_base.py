from abc import abstractmethod
from dataclasses import dataclass
from typing import Iterable

from datahub.ingestion.source.state.stateful_ingestion_base import StatefulIngestionSourceBase


# See https://github.com/python/mypy/issues/5374 for why we suppress this mypy error.
@dataclass  # type: ignore[misc]
class SampleableStatefulIngestionSourceBase(StatefulIngestionSourceBase):
    @abstractmethod
    def sample(self, schema_name: str) -> Iterable[str]:
        pass
