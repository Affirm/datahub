import datetime
import os
from typing import Iterable, List, cast
from unittest import TestCase
from unittest.mock import ANY, MagicMock, Mock, call, patch

import boto3
import pytest
import requests
from botocore.stub import Stubber

from datahub.classification.classifier_pipeline import ClassifierPipeline
from datahub.ingestion.api.common import PipelineContext, RecordEnvelope, WorkUnit
from datahub.ingestion.api.sampleable_source import SampleableSource
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.transform import Transformer
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.common import Status
from datahub.metadata.com.linkedin.pegasus2avro.mxe import SystemMetadata
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    DatasetSnapshotClass,
    MetadataChangeEventClass,
    SchemaMetadataClass,
)
from datahub.utilities.metrics import (
    CloudWatchDatahubCustomMetricReporter,
    DatahubCustomEnvironment,
    DatahubCustomMetric,
    DatahubCustomMetricReporter,
    DatahubCustomNamespace,
)
from tests.test_helpers.sink_helpers import RecordingSinkReport


class ClassifierPipelineTest(TestCase):
    def setUp(self):
        self.ddb_client = boto3.client("dynamodb")
        self.metric_reporter: DatahubCustomMetricReporter = (
            CloudWatchDatahubCustomMetricReporter(
                Mock(),
                DatahubCustomEnvironment.MYSQL,
                DatahubCustomNamespace.DATAHUB_PII_CLASSIFICATION,
            )
        )
        self.metric_reporter.zero = MagicMock()
        self.metric_reporter.increment = MagicMock()
        self.metric_reporter.duration_millis = MagicMock()

        self.default_pipeline_config = {
            "datahub_base_url": "http://localhost:1234",
            "datahub_username": "dummyUser",
            "datahub_password": "abc",
            "num_shards": 1,
            "pii_classification_state_table_name": "DummyPiiTable",
            "shard_id": 0,
            "source": {
                "type": "tests.unit.test_classification_pipeline.FakeSingleSourceWithPii",
                "config": {
                    "sampling_query_template": "SELECT * from {} LIMIT 5",
                },
            },
            "transformers": [
                {
                    "type": "tests.unit.test_classification_pipeline.AddStatusRemovedTransformer"
                }
            ],
        }

        # TODO file path temporary until classifier logic ported into lib
        privacy_mapping_file_path = "/tmp/mapping.csv"
        os.environ["PRIVACY_TYPE_MAPPING_FILE_PATH"] = privacy_mapping_file_path
        with open(privacy_mapping_file_path, "w+") as f:
            f.write("Type,Description,Requirements,Note")
            f.write('PERSON,"People, including fictional.","GLBA')
            f.write("CCPA")
            f.write('PIPEDA",')

        self.success_attach_term_response = "{addTerm: true}"
        self.sample_urn = "urn:li:dataset:(urn:li:dataPlatform:test_platform,test,PROD)"
        fake_responses = [Mock(), MagicMock(), MagicMock(), MagicMock()]
        mock_cookies = Mock()
        mock_dict = Mock()
        mock_cookies.get_dict = mock_dict
        mock_dict.return_value = {
            "PLAY_SESSION": "0e5d4daecf942033c2054844c3d2dce4b1115ef0-actor=urn%3Ali%3Acorpuser%3AdummyUser",
            "actor": "urn:li:corpuser:dummyUser",
        }
        fake_responses[0].cookies = mock_cookies
        fake_responses[1].text = self.success_attach_term_response
        fake_responses[2].text = self.success_attach_term_response
        fake_responses[3].text = self.success_attach_term_response
        self.default_fake_responses = fake_responses

    def _assert_metric_reporter_zeroed(self):
        expected_calls = [
            call(DatahubCustomMetric.NEW_DATASET_CLASSIFIED),
            call(DatahubCustomMetric.NEW_DATASET_CLASSIFIED_AS_PII),
            call(DatahubCustomMetric.EXISTING_DATASET_CLASSIFIED),
            call(DatahubCustomMetric.EXISTING_DATASET_CLASSIFIED_AS_PII),
            call(DatahubCustomMetric.DATASET_CLASSIFICATION_SKIPPED),
            call(DatahubCustomMetric.DATASET_CLASSIFICATION_FAILED),
        ]
        self.metric_reporter.zero.assert_has_calls(expected_calls, any_order=True)
        self.assertEqual(len(expected_calls), self.metric_reporter.zero.call_count)

    def _assert_metric_calls(self, increment_expected_calls, duration_expected_calls):
        self._assert_metric_reporter_zeroed()
        self.metric_reporter.increment.assert_has_calls(
            increment_expected_calls, any_order=True
        )
        self.metric_reporter.duration_millis.assert_has_calls(
            duration_expected_calls, any_order=True
        )
        self.assertEqual(
            len(increment_expected_calls), self.metric_reporter.increment.call_count
        )
        self.assertEqual(
            len(duration_expected_calls),
            self.metric_reporter.duration_millis.call_count,
        )

    @patch.object(requests, "post")
    def test_run_classifyNew_createsClassifications(self, post_mock):
        post_mock.side_effect = self.default_fake_responses

        sd = Stubber(self.ddb_client)
        sd.add_response("query", {"Items": []})
        sd.add_response("put_item", {})
        sd.activate()

        ClassifierPipeline.create(
            self.default_pipeline_config,
            self.ddb_client,
            self.metric_reporter,
        ).run()

        increment_expected_calls = [
            call(DatahubCustomMetric.NEW_DATASET_CLASSIFIED),
            call(DatahubCustomMetric.NEW_DATASET_CLASSIFIED_AS_PII),
        ]

        duration_expected_calls = [
            call(DatahubCustomMetric.SINGLE_DATASET_CLASSIFICATION_TIME, ANY),
            call(DatahubCustomMetric.ALL_DATASETS_CLASSIFICATION_TIME, ANY),
        ]

        self._assert_metric_calls(increment_expected_calls, duration_expected_calls)

        sd.assert_no_pending_responses()

    @patch.object(requests, "post")
    def test_run_classifyMultipleNew_createsClassifications(self, post_mock):
        fake_responses = [
            Mock(),
            MagicMock(),
            MagicMock(),
            MagicMock(),
            MagicMock(),
            MagicMock(),
            MagicMock(),
        ]
        # 0 is login response request/response
        fake_responses[0] = self.default_fake_responses[0]
        fake_responses[1].text = self.success_attach_term_response
        fake_responses[2].text = self.success_attach_term_response
        fake_responses[3].text = self.success_attach_term_response
        fake_responses[4].text = self.success_attach_term_response
        fake_responses[5].text = self.success_attach_term_response
        fake_responses[6].text = self.success_attach_term_response
        post_mock.side_effect = fake_responses

        multi_source_config = self.default_pipeline_config.copy()
        multi_source_config["source"] = {
            "type": "tests.unit.test_classification_pipeline.FakeMultiSourceWithPii",
            "config": {
                "sampling_query_template": "SELECT * from {} LIMIT 5",
            },
        }

        sd = Stubber(self.ddb_client)
        sd.add_response("query", {"Items": []})
        sd.add_response("put_item", {})
        sd.add_response("query", {"Items": []})
        sd.add_response("put_item", {})
        sd.activate()

        ClassifierPipeline.create(
            multi_source_config, self.ddb_client, self.metric_reporter
        ).run()

        increment_expected_calls = [
            call(DatahubCustomMetric.NEW_DATASET_CLASSIFIED),
            call(DatahubCustomMetric.NEW_DATASET_CLASSIFIED),
            call(DatahubCustomMetric.NEW_DATASET_CLASSIFIED_AS_PII),
            call(DatahubCustomMetric.NEW_DATASET_CLASSIFIED_AS_PII),
        ]

        duration_expected_calls = [
            call(DatahubCustomMetric.SINGLE_DATASET_CLASSIFICATION_TIME, ANY),
            call(DatahubCustomMetric.SINGLE_DATASET_CLASSIFICATION_TIME, ANY),
            call(DatahubCustomMetric.ALL_DATASETS_CLASSIFICATION_TIME, ANY),
        ]

        self._assert_metric_calls(increment_expected_calls, duration_expected_calls)

        sd.assert_no_pending_responses()

    @patch.object(requests, "post")
    def test_run_classifyExisting_updatesClassifications(self, post_mock):
        post_mock.side_effect = self.default_fake_responses

        sd = Stubber(self.ddb_client)
        sd.add_response(
            "query",
            {
                "Items": [
                    {
                        "pk": {"S": self.sample_urn},
                        "sk": {"S": "DATASET"},
                        "createdDate": {"S": "2021-11-15T21:46:38.336954"},
                        "lastEvalDate": {"S": "2021-11-15T21:46:38.336954"},
                    }
                ]
            },
        )
        sd.add_response("update_item", {})
        sd.activate()

        ClassifierPipeline.create(
            self.default_pipeline_config,
            self.ddb_client,
            self.metric_reporter,
        ).run()

        increment_expected_calls = [
            call(DatahubCustomMetric.EXISTING_DATASET_CLASSIFIED),
            call(DatahubCustomMetric.EXISTING_DATASET_CLASSIFIED_AS_PII),
        ]

        duration_expected_calls = [
            call(DatahubCustomMetric.SINGLE_DATASET_CLASSIFICATION_TIME, ANY),
            call(DatahubCustomMetric.ALL_DATASETS_CLASSIFICATION_TIME, ANY),
        ]

        self._assert_metric_calls(increment_expected_calls, duration_expected_calls)

        sd.assert_no_pending_responses()

    @patch.object(requests, "post")
    def test_run_classifyExistingTtlNotExpired_doesNothing(self, post_mock):
        post_mock.side_effect = self.default_fake_responses

        sd = Stubber(self.ddb_client)
        sd.add_response(
            "query",
            {
                "Items": [
                    {
                        "pk": {"S": self.sample_urn},
                        "sk": {"S": "DATASET"},
                        "createdDate": {"S": "2021-11-15T21:46:38.336954"},
                        "lastEvalDate": {"S": datetime.datetime.now().isoformat()},
                    }
                ]
            },
        )
        sd.activate()

        ClassifierPipeline.create(
            self.default_pipeline_config,
            self.ddb_client,
            self.metric_reporter,
        ).run()

        increment_expected_calls = [
            call(DatahubCustomMetric.DATASET_CLASSIFICATION_SKIPPED)
        ]

        duration_expected_calls = [
            call(DatahubCustomMetric.ALL_DATASETS_CLASSIFICATION_TIME, ANY)
        ]

        self._assert_metric_calls(increment_expected_calls, duration_expected_calls)

        sd.assert_no_pending_responses()

    @patch.object(requests, "post")
    def test_run_classifyNewAndNotAllRequestsSucceed_doesNotUpdateDynamoState(
        self, post_mock
    ):
        fake_responses = [Mock(), MagicMock(), MagicMock(), MagicMock()]
        # 0 is login response request/response
        fake_responses[0] = self.default_fake_responses[0]
        fake_responses[1].text = self.success_attach_term_response
        fake_responses[2] = requests.exceptions.ConnectionError()
        fake_responses[3].text = self.success_attach_term_response
        post_mock.side_effect = fake_responses

        sd = Stubber(self.ddb_client)
        sd.add_response("query", {"Items": []})
        sd.activate()

        ClassifierPipeline.create(
            self.default_pipeline_config,
            self.ddb_client,
            self.metric_reporter,
        ).run()

        increment_expected_calls = [
            call(DatahubCustomMetric.DATASET_CLASSIFICATION_FAILED),
        ]

        duration_expected_calls = [
            call(DatahubCustomMetric.ALL_DATASETS_CLASSIFICATION_TIME, ANY)
        ]

        self._assert_metric_calls(increment_expected_calls, duration_expected_calls)

        sd.assert_no_pending_responses()

    @patch.object(requests, "post")
    def test_run_classifyExistingGivesNoResults_updatesDynamoState(self, post_mock):
        no_pii_pipeline_config = self.default_pipeline_config.copy()
        no_pii_pipeline_config["source"] = {
            "type": "tests.unit.test_classification_pipeline.FakeSingleSourceWithoutPii"
        }

        fake_responses = [Mock()]
        fake_responses[0] = self.default_fake_responses[0]
        post_mock.side_effect = fake_responses

        sd = Stubber(self.ddb_client)
        sd.add_response(
            "query",
            {
                "Items": [
                    {
                        "pk": {"S": self.sample_urn},
                        "sk": {"S": "DATASET"},
                        "createdDate": {"S": "2021-11-15T21:46:38.336954"},
                        "lastEvalDate": {"S": "2021-11-15T21:46:38.336954"},
                    }
                ]
            },
        )
        sd.add_response("update_item", {})
        sd.activate()

        ClassifierPipeline.create(
            no_pii_pipeline_config,
            self.ddb_client,
            self.metric_reporter,
        ).run()

        increment_expected_calls = [
            call(DatahubCustomMetric.EXISTING_DATASET_CLASSIFIED)
        ]

        duration_expected_calls = [
            call(DatahubCustomMetric.SINGLE_DATASET_CLASSIFICATION_TIME, ANY),
            call(DatahubCustomMetric.ALL_DATASETS_CLASSIFICATION_TIME, ANY),
        ]

        self._assert_metric_calls(increment_expected_calls, duration_expected_calls)

        sd.assert_no_pending_responses()

    @patch.object(requests, "post")
    def test_run_classifyNewDoesntBelongToShard_skipsClassification(self, post_mock):
        # 0 is login response request/response
        post_mock.side_effect = [self.default_fake_responses[0]]

        sd = Stubber(self.ddb_client)
        sd.add_response("query", {"Items": []})
        sd.activate()

        # Consistent hash of "test" (the test schema name) is 44363272, since 44363272 % 2 == 0, shard 1 should ignore it
        two_shards_config = self.default_pipeline_config.copy()
        two_shards_config["shard_id"] = 1
        two_shards_config["num_shards"] = 2

        ClassifierPipeline.create(
            two_shards_config, self.ddb_client, self.metric_reporter
        ).run()

        increment_expected_calls = []

        duration_expected_calls = [
            call(DatahubCustomMetric.ALL_DATASETS_CLASSIFICATION_TIME, ANY)
        ]

        self._assert_metric_calls(increment_expected_calls, duration_expected_calls)

        sd.assert_no_pending_responses()


class AddStatusRemovedTransformer(Transformer):
    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Transformer":
        return cls()

    def transform(
        self, record_envelopes: Iterable[RecordEnvelope]
    ) -> Iterable[RecordEnvelope]:
        for record_envelope in record_envelopes:
            record_envelope.record.proposedSnapshot.aspects.append(
                get_status_removed_aspect()
            )
            yield record_envelope


class FakeSingleSourceWithPii(SampleableSource):
    def __init__(self):
        self.source_report = SourceReport()
        self.work_units: List[MetadataWorkUnit] = [
            MetadataWorkUnit(id="workunit-1", mce=get_initial_mce("test"))
        ]

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Source":
        return FakeSingleSourceWithPii()

    @classmethod
    def sample(self, schema_name: str) -> dict:
        return {
            "foo": ["abc", "def"],
            "bar": ["testuser1@affirm.com", "testuser2@affirm.com"],
        }

    def get_workunits(self) -> Iterable[WorkUnit]:
        return self.work_units

    def get_report(self) -> SourceReport:
        return self.source_report

    def close(self):
        pass


class FakeMultiSourceWithPii(SampleableSource):
    def __init__(self):
        self.source_report = SourceReport()
        self.work_units: List[MetadataWorkUnit] = [
            MetadataWorkUnit(id="workunit-1", mce=get_initial_mce("test1")),
            MetadataWorkUnit(id="workunit-2", mce=get_initial_mce("test2")),
        ]

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Source":
        return FakeMultiSourceWithPii()

    @classmethod
    def sample(self, schema_name: str) -> dict:
        return {
            "foo": ["abc", "def"],
            "bar": ["testuser1@affirm.com", "testuser2@affirm.com"],
        }

    def get_workunits(self) -> Iterable[WorkUnit]:
        return self.work_units

    def get_report(self) -> SourceReport:
        return self.source_report

    def close(self):
        pass


class FakeSingleSourceWithoutPii(SampleableSource):
    def __init__(self):
        self.source_report = SourceReport()
        self.work_units: List[MetadataWorkUnit] = [
            MetadataWorkUnit(id="workunit-1", mce=get_initial_mce("test"))
        ]

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Source":
        return FakeSingleSourceWithoutPii()

    @classmethod
    def sample(self, schema_name: str) -> dict:
        return {"foo": ["abc", "def"], "bar": ["ghi", "jkl"]}

    def get_workunits(self) -> Iterable[WorkUnit]:
        return self.work_units

    def get_report(self) -> SourceReport:
        return self.source_report

    def close(self):
        pass


def get_initial_mce(schema_name: str) -> MetadataChangeEventClass:
    return MetadataChangeEventClass(
        proposedSnapshot=DatasetSnapshotClass(
            urn=f"urn:li:dataset:(urn:li:dataPlatform:test_platform,{schema_name},PROD)",
            aspects=[
                DatasetPropertiesClass(
                    description=f"{schema_name}.description",
                ),
                SchemaMetadataClass(
                    schemaName=f"{schema_name}",
                    platform="urn:li:dataPlatform:test_platform",
                    version=0,
                    hash="",
                    platformSchema={},
                    fields=[],
                ),
            ],
        ),
        systemMetadata=SystemMetadata(
            lastObserved=1586847600000, runId="pipeline_test"
        ),
    )


def get_status_removed_aspect() -> Status:
    return Status(removed=False)
