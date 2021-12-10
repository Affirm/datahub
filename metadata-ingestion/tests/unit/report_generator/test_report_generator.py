import os
import tempfile
from unittest import TestCase
from unittest.mock import patch, MagicMock

from datahub.cli.generate_report.report_generator import ReportGenerator


FIXTURES_PATH = os.path.dirname(__file__)

class TestReportGenerator(TestCase):

    def setUp(self):
        self.tmp_output_file = tempfile.NamedTemporaryFile()
        self.config = {
            "datahub_base_url": "http://localhost:1234",
            "search_queries": ['*'],
            "output": {
                "format": "csv",
                "destination": {
                    "filepath": self.tmp_output_file.name
                },
            },
        }

    def tearDown(self):
        self.tmp_output_file.close()

    @patch('datahub.cli.generate_report.report_generator.PrivacyTermExtractor')
    def test_basic(self, mock_privacy_term_extractor_class):
        mock_extractor = MagicMock()
        mock_extractor.yield_search_results.return_value = [
            {
                'dataset': 'stage_db.users.user',
                'field': 'id',
                'type': ['IDENTIFIER'],
                'privacy_law': ['CCPA', 'GDPR'],
            },
            {
                'dataset': 'stage_db.users.user',
                'field': 'full_name',
                'type': ['PERSON'],
                'privacy_law': ['CCPA', 'GDPR'],
            },
            {
                'dataset': 'stage_db.users.user',
                'field': 'created',
                'type': ['DATE'],
                'privacy_law': [],
            },
            {
                'dataset': 'stage_db.users.user',
                'field': 'updated',
                'type': ['DATE'],
                'privacy_law': [],
            },
            {
                'dataset': 'stage_db.users.user',
                'field': 'field_without_terms',
                'type': [],
                'privacy_law': [],
            },
        ]
        mock_privacy_term_extractor_class.return_value = mock_extractor

        rg = ReportGenerator.create(self.config)
        rg.generate()

        mock_privacy_term_extractor_class.assert_called_once_with(self.config['datahub_base_url'])
        mock_extractor.yield_search_results_assert_called_once_with(self.config['search_queries'])

        with open(os.path.join(FIXTURES_PATH, 'expected_basic.csv'), 'rb') as f:
            expected_lines = f.readlines()

        self.tmp_output_file.seek(0)
        actual_lines = self.tmp_output_file.readlines()
        for expected, actual in zip(expected_lines, actual_lines):
            self.assertEqual(expected.strip(), actual.strip())
