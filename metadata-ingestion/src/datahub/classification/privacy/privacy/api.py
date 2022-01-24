from typing import List, Optional, Any
from datahub.classification.privacy.privacy.detectors import (
    NameDetector,
    AddressDetector,
    MACAddressDetector,
    DOBDetector,
    SSNDetector,
    EmailDetector,
    PhoneDetector,
    OrganizationDetector
)
from datahub.classification.privacy.privacy.types import EntityType, DB_COLUMN
from datahub.classification.privacy.privacy.defs import SENTINEL


class PIIEngine:
    detectors = [
        NameDetector,
        AddressDetector,
        MACAddressDetector,
        DOBDetector,
        SSNDetector,
        EmailDetector,
        PhoneDetector,
        OrganizationDetector
    ]

    def apply_detectors(self, db_col) -> EntityType:
        entity_types = []

        for detector in self.detectors:
            entity_type = detector().run_detection(db_col)
            if entity_type != EntityType.UNDECIDED:
                entity_types.append(entity_type)

        return entity_types

    def detect_entity_types(
        self,
        samples: List[Any] = [],
        database: Optional[str]=None,
        schema: Optional[str]=None,
        table: Optional[str]=None,
        column: Optional[str]=None,
    ):
        db_col = DB_COLUMN(database=database if database else SENTINEL,
                           schema=schema if schema else SENTINEL,
                           table=table if table else SENTINEL,
                           column=column if column else SENTINEL,
                           samples=samples if samples is not None else [])

        all_types = self.apply_detectors(db_col=db_col)
        all_types_str = [r.value for r in all_types]
        return all_types_str
