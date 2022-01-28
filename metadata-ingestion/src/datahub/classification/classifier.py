import logging
import os
import re
from typing import Dict, Set

import pandas as pd
import spacy

from datahub.classification.privacy.privacy.api import PIIEngine
from datahub.ingestion.api.common import RecordEnvelope
from datahub.ingestion.source.state.sampleable_stateful_ingestion_base import (
    SampleableStatefulIngestionSourceBase,
)

logger: logging.Logger = logging.getLogger(__name__)


class ClassificationResult:
    privacy_laws: Set[str] = set()
    pii_types: Set[str] = set()

    def __init__(self):
        self.privacy_laws = set()
        self.pii_types = set()


class Classifier:
    sampler: SampleableStatefulIngestionSourceBase

    def __init__(self, sampler: SampleableStatefulIngestionSourceBase):
        self.sampler = sampler
        # TODO https://jira.team.affirm.com/browse/DF-1737
        self.engine = PIIEngine()

    def classify(self, schema_name: str, urn: str) -> Dict[str, ClassificationResult]:
        dataset_res = {}
        sample = self.sampler.sample(schema_name)
        for col in sample:
            samples = list(filter(lambda a : isinstance(a, str), sample[col]))
            resulting_pii_types = self.engine.detect_entity_types(samples)
            if resulting_pii_types:
                dataset_res[col] = ClassificationResult()
                # TODO https://jira.team.affirm.com/browse/DF-1841
                dataset_res[col].privacy_laws = ["GDPR"]
                dataset_res[col].pii_types = resulting_pii_types
        return dataset_res
