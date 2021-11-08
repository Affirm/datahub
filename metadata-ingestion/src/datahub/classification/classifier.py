import logging
import os
import re
from typing import Dict, Set

import pandas as pd
import spacy

from datahub.ingestion.api.common import RecordEnvelope
from datahub.ingestion.api.sampleable_source import SampleableSource

logger: logging.Logger = logging.getLogger(__name__)

nlp = spacy.load("en_core_web_sm")


class ClassificationResult:
    privacy_laws: Set[str] = set()
    pii_types: Set[str] = set()

    def __init__(self):
        self.privacy_laws = set()
        self.pii_types = set()


class Classifier:
    sampler: SampleableSource

    def __init__(self, sampler: SampleableSource):
        self.sampler = sampler

        # TODO scrap file import/reading in favor of a lib https://jira.team.affirm.com/browse/DF-1737

        privacy_type_mapping_filename = os.environ["PRIVACY_TYPE_MAPPING_FILE_PATH"]
        self.privacy_type_mapping = pd.read_csv(
            privacy_type_mapping_filename,
            index_col=0,
            keep_default_na=False,
            converters={"Requirements": lambda x: x.split("\n") if x else None},
        ).to_dict("index")

    def classify(self, schema_name: str, urn: str) -> Dict[str, ClassificationResult]:
        dataset_res = {}
        sample = self.sampler.sample(schema_name)
        for col in sample:
            for val in sample[col]:
                if isinstance(val, str):
                    class_res = self._classify(val)
                    if class_res:
                        if col not in dataset_res:
                            dataset_res[col] = ClassificationResult()
                        dataset_res[col].privacy_laws = set.union(
                            class_res.privacy_laws, dataset_res[col].privacy_laws
                        )
                        dataset_res[col].pii_types = set.union(
                            class_res.pii_types, dataset_res[col].pii_types
                        )
        return dataset_res

    # TODO scrap all below in favor of a lib https://jira.team.affirm.com/browse/DF-1737

    def _classify(self, text: str) -> ClassificationResult:
        nlp_doc = nlp(text)

        data_matchings = []

        for entity in nlp_doc.ents:
            if (
                entity.label_ in self.privacy_type_mapping
                and self.privacy_type_mapping[entity.label_]["Requirements"]
            ):
                data_matching_object = {
                    "type": entity.label_,
                    "value": entity.text,
                    "requirements": self.privacy_type_mapping[entity.label_][
                        "Requirements"
                    ],
                }
                data_matchings.append(data_matching_object)

        for extracted in self.extract_email(nlp_doc.text) + self.extract_phone(
            nlp_doc.text
        ):
            data_matching_object = {
                "type": "CONTACT",
                "value": extracted,
                "requirements": ["GLBA", "CCPA", "PIPEDA"],
            }
            data_matchings.append(data_matching_object)

        for extracted in self.extract_chd(nlp_doc.text):
            data_matching_object = {
                "type": "CHD",
                "value": extracted,
                "requirements": ["PCI"],
            }
            data_matchings.append(data_matching_object)

        data_result = {
            "match": bool(data_matchings),
            "matchings": data_matchings,
        }

        if not data_result["matchings"]:
            return None
        else:
            res = ClassificationResult()
            for match in data_result["matchings"]:
                res.pii_types.add(match["type"])
                for law in match["requirements"]:
                    if law in ["CCPA", "APP", "PIPEDA"]:
                        res.privacy_laws.add(law)
            return res

    def extract_email(self, text):
        return re.findall("[A-Za-z0-9]+[A-Za-z0-9._%+-]*@\w+.\w{2,4}", text)

    def extract_phone(self, text):
        return re.findall(
            "(\d{3}[-\.\s]??\d{3}[-\.\s]??\d{4}|\(\d{3}\)\s*\d{3}[-\.\s]??\d{4}|\d{3}[-\.\s]??\d{4})",
            text,
        )

    def extract_chd(self, text):
        return re.findall("[0-9]{16}", text)
