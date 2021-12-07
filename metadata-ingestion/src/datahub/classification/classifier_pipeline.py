import base64
import datetime
import hashlib
import logging
import uuid
from typing import Any, Dict, Iterable, List, Optional

import click
import requests

from datahub.classification.classifier import ClassificationResult, Classifier
from datahub.configuration.common import ConfigModel, DynamicTypedConfig
from datahub.ingestion.api.common import PipelineContext, RecordEnvelope
from datahub.ingestion.api.sampleable_source import SampleableSource
from datahub.ingestion.api.source import Extractor
from datahub.ingestion.api.transform import Transformer
from datahub.ingestion.extractor.extractor_registry import extractor_registry
from datahub.ingestion.source.source_registry import source_registry
from datahub.ingestion.transformer.transform_registry import transform_registry
from datahub.metadata.schema_classes import SchemaMetadataClass

logger = logging.getLogger(__name__)


class SourceConfig(DynamicTypedConfig):
    extractor: str = "generic"


class ClassifierPipelineConfig(ConfigModel):
    datahub_base_url: str
    datahub_username: str
    datahub_encrypted_password: str
    ddb_client: Any  # Type hints not available for boto3 clients
    kms_client: Any  # Type hints not available for boto3 clients
    num_shards: int
    pii_classification_state_table_name: str
    shard_id: int
    source: SourceConfig
    transformers: Optional[List[DynamicTypedConfig]]


class ClassifierPipeline:
    DATAHUB_GRAPHQL_ENDPOINT: str = "/api/v2/graphql"
    DATAHUB_LOGIN_ENDPOINT: str = "/logIn"
    DATASET_RECORD_SK: str = "DATASET"

    config: ClassifierPipelineConfig
    ctx: PipelineContext
    classifier: Classifier
    datahub_base_url: str
    datahub_cookies: Dict[str, str]
    my_shard_id: int
    num_shards: int
    source: SampleableSource
    transformers: List[Transformer]

    def __init__(self, config: ClassifierPipelineConfig):
        logger.info(f"Building classifier pipeline from config: {config}")
        self.config = config
        self.ctx = PipelineContext(run_id=str(uuid.uuid1()))
        source_type = self.config.source.type
        source_class = source_registry.get(source_type)
        self.source: SampleableSource = source_class.create(
            self.config.source.dict().get("config", {}), self.ctx
        )
        self.extractor_class = extractor_registry.get(self.config.source.extractor)

        self.ddb_client = config.ddb_client
        self.kms_client = config.kms_client
        self.classifier = Classifier(self.source)
        self.pii_classification_state_table_name = (
            config.pii_classification_state_table_name
        )
        self.datahub_base_url = config.datahub_base_url
        self.datahub_cookies = self._login_to_datahub(
            config.datahub_username, config.datahub_encrypted_password
        )
        self.my_shard_id = config.shard_id
        self.num_shards = config.num_shards
        assert self.my_shard_id >= 0 and self.my_shard_id < self.num_shards

        self._configure_transforms()

    def _login_to_datahub(
        self, username: str, encrypted_password: str
    ) -> Dict[str, str]:
        kms_result = self.kms_client.decrypt(
            CiphertextBlob=bytes(base64.b64decode(encrypted_password))
        )
        password = kms_result["Plaintext"].decode("UTF-8")

        url = "{}{}".format(self.datahub_base_url, self.DATAHUB_LOGIN_ENDPOINT)
        creds = {"username": username, "password": password}

        return requests.post(url, json=creds).cookies.get_dict()

    def _configure_transforms(self) -> None:
        self.transformers = []
        if self.config.transformers is not None:
            for transformer in self.config.transformers:
                transformer_type = transformer.type
                transformer_class = transform_registry.get(transformer_type)
                transformer_config = transformer.dict().get("config", {})
                self.transformers.append(
                    transformer_class.create(transformer_config, self.ctx)
                )
                logger.debug(
                    f"Transformer type:{transformer_type},{transformer_class} configured"
                )

    def _get_schema_name(self, record: RecordEnvelope) -> str:
        res = list(
            filter(
                lambda x: isinstance(x, SchemaMetadataClass),
                record.record.proposedSnapshot.aspects,
            )
        )
        return res[0].schemaName

    def _consistent_hash(self, s: str) -> int:
        # these hashes will be the same across runs / processes, as opposed to python's built in `hash` function
        return abs(int(hashlib.sha256(s.encode("utf-8")).hexdigest(), 16) % 10 ** 8)

    def _get_dataset_pii_classification_state_record(self, urn: str):
        response = self.ddb_client.query(
            TableName=self.pii_classification_state_table_name,
            KeyConditionExpression="pk = :urn and sk = :rangekey",
            ExpressionAttributeValues={
                ":urn": {"S": urn},
                ":rangekey": {"S": self.DATASET_RECORD_SK},
            },
        )
        return None if len(response["Items"]) == 0 else response["Items"][0]

    def _write_classification_result(
        self,
        urn: str,
        classification_result: Dict[str, ClassificationResult],
        is_first_time_classified: bool,
    ):
        now = datetime.datetime.now()

        are_writes_successful = True
        for col_name, class_result in classification_result.items():
            for law in class_result.privacy_laws:
                are_writes_successful &= self._attach_glossary_terms(
                    urn, col_name, "PrivacyLaw.{}".format(law)
                )
            for type in class_result.pii_types:
                are_writes_successful &= self._attach_glossary_terms(
                    urn, col_name, "PiiData.{}".format(type)
                )

        if are_writes_successful:
            if is_first_time_classified:
                self.ddb_client.put_item(
                    TableName=self.pii_classification_state_table_name,
                    Item={
                        "pk": {"S": urn},
                        "sk": {"S": self.DATASET_RECORD_SK},
                        "createdDate": {"S": now.isoformat()},
                        "lastEvalDate": {"S": now.isoformat()},
                    },
                )
            else:
                self.ddb_client.update_item(
                    TableName=self.pii_classification_state_table_name,
                    Key={"pk": {"S": urn}, "sk": {"S": self.DATASET_RECORD_SK}},
                    UpdateExpression="SET lastEvalDate = :newEvalDate",
                    ExpressionAttributeValues={":newEvalDate": {"S": now.isoformat()}},
                )
        else:
            logger.warning(f"Not all writes successful for urn {urn}, skipping state table update")

    def _attach_glossary_terms(
        self, urn: str, subfield: str, glossary_term: str
    ) -> bool:

        url = "{}{}".format(self.datahub_base_url, self.DATAHUB_GRAPHQL_ENDPOINT)

        req = {
            "operationName": "addTerm",
            "variables": {
                "input": {
                    "termUrn": "urn:li:glossaryTerm:{}".format(glossary_term),
                    "resourceUrn": urn,
                    "subResource": subfield,
                    "subResourceType": "DATASET_FIELD",
                }
            },
            "query": "mutation addTerm($input: TermAssociationInput!) {\n  addTerm(input: $input)\n}\n",
        }

        r = requests.post(url, json=req, cookies=self.datahub_cookies)

        success = "addTerm" in r.text

        if success:
            logger.info(
                "Attached term {} to {} on field {} with code {}".format(
                    glossary_term, urn, subfield, str(r.status_code)
                )
            )
        else:
            logger.error(
                "Failed to attach term {} to {} on field {} with code {}".format(
                    glossary_term, urn, subfield, str(r.status_code)
                )
            )

        return success

    @classmethod
    def create(cls, config_dict: dict) -> "ClassifierPipeline":
        config = ClassifierPipelineConfig.parse_obj(config_dict)
        return cls(config)

    def run(self) -> None:
        extractor: Extractor = self.extractor_class()
        for wu in self.source.get_workunits():
            extractor.configure({}, self.ctx)

            for record_envelope in self.transform(extractor.get_records(wu)):
                urn = record_envelope.record.proposedSnapshot.urn
                schema_name = self._get_schema_name(record_envelope)
                schema_name_hash = self._consistent_hash(schema_name)
                does_record_belong_to_shard = (
                    schema_name_hash % self.num_shards == self.my_shard_id
                )

                record = self._get_dataset_pii_classification_state_record(urn)
                # TODO one verified working in prod E2E, change to 1 week TTL https://jira.team.affirm.com/browse/DF-1737
                is_ttl_expired = record is None or (
                    (
                        datetime.datetime.fromisoformat(record["lastEvalDate"]["S"])
                        + datetime.timedelta(seconds=30)
                    )
                    < datetime.datetime.now()
                )
                if does_record_belong_to_shard and is_ttl_expired:
                    try:
                        classification_res = self.classifier.classify(schema_name, urn)
                        self._write_classification_result(
                            urn, classification_res, record is None
                        )
                    except:
                        logger.exception(
                            "Classification pipeline failed for {}".format(schema_name)
                        )

            extractor.close()
        self.source.close()

    def transform(self, records: Iterable[RecordEnvelope]) -> Iterable[RecordEnvelope]:
        """
        Transforms the given sequence of records by passing the records through the transformers
        :param records: the records to transform
        :return: the transformed records
        """
        for transformer in self.transformers:
            records = transformer.transform(records)

        return records

    def pretty_print_summary(self) -> int:
        click.echo()
        click.secho(f"Source ({self.config.source.type}) report:", bold=True)
        click.echo(self.source.get_report().as_string())
        click.echo()
        if self.source.get_report().failures:
            click.secho("Pipeline finished with failures", fg="bright_red", bold=True)
            return 1
        elif self.source.get_report().warnings:
            click.secho("Pipeline finished with warnings", fg="yellow", bold=True)
            return 0
        else:
            click.secho("Pipeline finished successfully", fg="green", bold=True)
            return 0
