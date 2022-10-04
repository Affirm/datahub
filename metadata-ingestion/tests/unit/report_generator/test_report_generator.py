import os
import tempfile
from unittest import TestCase
from unittest.mock import MagicMock, call, patch

import requests

import datahub.cli.generate_report.report_generator
from datahub.cli.generate_report.report_generator import (
    PrivacyTermExtractor,
    ReportGenerator,
)

FIXTURES_PATH = os.path.dirname(__file__)


class TestReportGenerator(TestCase):
    def setUp(self):
        self.tmp_output_file = tempfile.NamedTemporaryFile()
        self.config = {
            "datahub_base_url": "http://localhost:1234",
            "datahub_token": "TOKEN",
            "search_queries": ["*"],
            "output": {
                "format": "csv",
                "destinations": [
                    {"filepath": self.tmp_output_file.name},
                ],
            },
        }

    def tearDown(self):
        self.tmp_output_file.close()

    @patch.object(datahub.cli.generate_report.report_generator, "PrivacyTermExtractor")
    def test_basic(self, mock_privacy_term_extractor_class):
        mock_extractor = MagicMock()
        mock_extractor.yield_search_results.return_value = [
            {
                "dataset": "stage_db.users.user",
                "field": "id",
                "type": ["IDENTIFIER"],
                "privacy_law": ["CCPA", "GDPR"],
            },
            {
                "dataset": "stage_db.users.user",
                "field": "full_name",
                "type": ["PERSON"],
                "privacy_law": ["CCPA", "GDPR"],
            },
            {
                "dataset": "stage_db.users.user",
                "field": "created",
                "type": ["DATE"],
                "privacy_law": [],
            },
            {
                "dataset": "stage_db.users.user",
                "field": "updated",
                "type": ["DATE"],
                "privacy_law": [],
            },
            {
                "dataset": "stage_db.users.user",
                "field": "field_without_terms",
                "type": [],
                "privacy_law": [],
            },
        ]
        mock_privacy_term_extractor_class.return_value = mock_extractor

        rg = ReportGenerator.create(self.config)
        rg.generate()

        mock_privacy_term_extractor_class.assert_called_once_with(
            self.config["datahub_base_url"],
            self.config["datahub_token"],
        )
        mock_extractor.yield_search_results_assert_called_once_with(
            self.config["search_queries"],
        )

        with open(os.path.join(FIXTURES_PATH, "expected_basic.csv"), "rb") as f:
            expected_lines = f.readlines()

        self.tmp_output_file.seek(0)
        actual_lines = self.tmp_output_file.readlines()
        for expected, actual in zip(expected_lines, actual_lines):
            self.assertEqual(expected.strip(), actual.strip())


class TestPrivacyTermExtractor(TestCase):
    @patch.object(requests, "post")
    def test_yield_search_results(self, mock_post):
        mock_search_results = [
            {
                "entity": {
                    "schemaMetadata": {
                        "name": "stage_db.users.user",
                        "fields": [
                            {"fieldPath": "first_name", "glossaryTerms": None},
                            {"fieldPath": "roles", "glossaryTerms": None},
                        ],
                    },
                    "editableSchemaMetadata": {
                        "editableSchemaFieldInfo": [
                            {
                                "fieldPath": "first_name",
                                "glossaryTerms": {
                                    "terms": [
                                        {
                                            "term": {
                                                "urn": "urn:li:glossaryTerm:PrivacyLaw.PIPEDA",
                                                "name": "PIPEDA",
                                            }
                                        },
                                        {
                                            "term": {
                                                "urn": "urn:li:glossaryTerm:PrivacyLaw.CCPA",
                                                "name": "CCPA",
                                            }
                                        },
                                        {
                                            "term": {
                                                "urn": "urn:li:glossaryTerm:PiiData.PERSON",
                                                "name": "PERSON",
                                            }
                                        },
                                        {
                                            "term": {
                                                "urn": "urn:li:glossaryTerm:PiiData.NORP",
                                                "name": "NORP",
                                            }
                                        },
                                    ]
                                },
                            }
                        ]
                    },
                }
            },
            {
                "entity": {
                    "schemaMetadata": {
                        "name": "stage_db.some_schema.some_event_table",
                        "fields": [
                            {"fieldPath": "event_id", "glossaryTerms": None},
                            {"fieldPath": "event_time", "glossaryTerms": None},
                        ],
                    },
                    "editableSchemaMetadata": {
                        "editableSchemaFieldInfo": [
                            {
                                "fieldPath": "event_time",
                                "glossaryTerms": {
                                    "terms": [
                                        {
                                            "term": {
                                                "urn": "urn:li:glossaryTerm:PiiData.DATE",
                                                "name": "DATE",
                                            }
                                        }
                                    ]
                                },
                            }
                        ]
                    },
                }
            },
        ]
        response1 = MagicMock()
        response1.json.return_value = {
            "data": {
                "search": {
                    "total": len(mock_search_results),
                    "searchResults": mock_search_results,
                },
            },
        }
        mock_post.side_effect = [response1]

        expected = [
            {
                "dataset": "stage_db.users.user",
                "field": "first_name",
                "type": ["PERSON", "NORP"],
                "privacy_law": ["PIPEDA", "CCPA"],
            },
            {
                "dataset": "stage_db.users.user",
                "field": "roles",
                "type": [],
                "privacy_law": [],
            },
            {
                "dataset": "stage_db.some_schema.some_event_table",
                "field": "event_id",
                "type": [],
                "privacy_law": [],
            },
            {
                "dataset": "stage_db.some_schema.some_event_table",
                "field": "event_time",
                "type": ["DATE"],
                "privacy_law": [],
            },
        ]
        extractor = PrivacyTermExtractor("http://localhost:1234", "TOKEN")

        actual = list(extractor.yield_search_results(["snowflake"]))
        mock_post.assert_called_once()
        self.assertListEqual(expected, actual)

    @patch.object(requests, "post")
    def test_yield_search_results__pagination(self, mock_post):
        def _make_entity(i):
            return {
                "entity": {
                    "schemaMetadata": {
                        "name": f"stage_db.some_schema.some_table_{i}",
                        "fields": [
                            {"fieldPath": "field1", "glossaryTerms": None},
                            {"fieldPath": "field2", "glossaryTerms": None},
                        ],
                    },
                    "editableSchemaMetadata": None,
                }
            }

        mock_search_results1 = [_make_entity(i) for i in range(0, 10)]
        mock_search_results2 = [_make_entity(i) for i in range(10, 17)]
        num_mock_datasets = len(mock_search_results1) + len(mock_search_results2)
        response1 = MagicMock()
        response1.json.return_value = {
            "data": {
                "search": {
                    "total": num_mock_datasets,
                    "searchResults": mock_search_results1,
                },
            },
        }
        response2 = MagicMock()
        response2.json.return_value = {
            "data": {
                "search": {
                    "total": num_mock_datasets,
                    "searchResults": mock_search_results2,
                },
            },
        }
        mock_post.side_effect = [response1, response2]

        extractor = PrivacyTermExtractor(
            "http://localhost:1234", "TOKEN", search_query_page_size=10
        )
        actual = list(extractor.yield_search_results(["snowflake"]))
        mock_post.assert_has_calls(
            [
                call(
                    "http://localhost:1234/api/graphql",
                    headers={
                        "Authorization": "Bearer TOKEN",
                    },
                    json={
                        "query": extractor.GRAPHQL_QUERY,
                        "variables": {
                            "search_query": "snowflake",
                            "page_size": 10,
                            "offset": 0,
                        },
                    },
                ),
                call(
                    "http://localhost:1234/api/graphql",
                    headers={
                        "Authorization": "Bearer TOKEN",
                    },
                    json={
                        "query": extractor.GRAPHQL_QUERY,
                        "variables": {
                            "search_query": "snowflake",
                            "page_size": 10,
                            "offset": 10,
                        },
                    },
                ),
            ]
        )
        num_mock_fields_per_dataset = 2
        self.assertEqual(len(actual), num_mock_datasets * num_mock_fields_per_dataset)

    @patch.object(requests, "post")
    def test_yield_search_results__empty_schema_metadata(self, mock_post):
        mock_search_results = [
            {
                "entity": {
                    "schemaMetadata": None,
                    "editableSchemaMetadata": None,
                }
            },
        ]
        response1 = MagicMock()
        response1.json.return_value = {
            "data": {
                "search": {
                    "total": len(mock_search_results),
                    "searchResults": mock_search_results,
                },
            },
        }
        mock_post.side_effect = [response1]

        expected = []
        extractor = PrivacyTermExtractor("http://localhost:1234", "TOKEN")

        actual = list(extractor.yield_search_results(["snowflake"]))
        mock_post.assert_called_once()
        self.assertListEqual(expected, actual)

    @patch.dict(os.environ, {"TOKEN": "hello"})
    def test_token_from_env_var(self):
        extractor = PrivacyTermExtractor("http://localhost:1234", "${TOKEN}")
        self.assertEqual(extractor.datahub_token, "hello")

    def test_missing_env_var_raises_exception(self):
        with self.assertRaises(ValueError):
            extractor = PrivacyTermExtractor("http://localhost:1234", "${TOKEN}")
