import csv
import datetime
import itertools
import logging
import os
import tempfile
from typing import Any, Dict, Generator, Iterable, TextIO

import boto3
import requests

from datahub.cli.generate_report.config import (
    FileOutput,
    GenerateReportConfig,
    OutputDestination,
    OutputFormatEnum,
    S3Output,
)

logger: logging.Logger = logging.getLogger(__name__)


class ReportGenerator:
    config: GenerateReportConfig

    def __init__(self, config: GenerateReportConfig) -> None:
        self.config = config

    def generate(self) -> int:
        extractor = PrivacyTermExtractor(self.config.datahub_base_url)
        rows = extractor.yield_search_results(self.config.search_queries)
        with OutputExporter(self.config.output.destination) as exporter:
            output_writer = OutputWriter(self.config.output.format, exporter.fileobj)
            output_writer.write(rows)
            exporter.export()
        return 0

    @classmethod
    def create(cls, config_dict: dict) -> "ReportGenerator":
        config = GenerateReportConfig.parse_obj(config_dict)
        return cls(config)

class PrivacyTermExtractor:
    DATAHUB_GRAPHQL_ENDPOINT: str = '/api/graphql'
    PAGE_SIZE: int = 500
    MAX_SEARCH_RESULTS_TOTAL: int = 100000
    GRAPHQL_QUERY: str = """
    query GetDatasetGlossaryTerms($search_query: String!, $page_size: Int, $offset:Int) {
      search(input: {
        type: DATASET,
        # search query cannot be empty, use "*" or by searching for a specific dataPlatoform name e.g. snowflake
        query: $search_query,
        count: $page_size,
        start: $offset,
      }) {
        total
        searchResults {
          entity {
            ... on Dataset {
              urn
              editableSchemaMetadata {
                editableSchemaFieldInfo {
                  fieldPath
                  glossaryTerms {
                    terms {
                      term {
                        urn
                        name
                      }
                    }
                  }
                }
              }
              # zero represents the latest version
              schemaMetadata(version: 0) {
                name
                fields {
                  fieldPath
                  glossaryTerms {
                    terms {
                      term {
                        urn
                        name
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    """
    graphql_api_url: str

    def __init__(self, datahub_base_url: str) -> None:
        self.graphql_api_url = f'{datahub_base_url}{self.DATAHUB_GRAPHQL_ENDPOINT}'

    def yield_search_results(self, search_queries: Iterable[str]) -> Generator[Dict, None, None]:
        return itertools.chain.from_iterable(
            self._yield_rows(entity)
            for search_query in search_queries
            for entity in self._yield_graphql_search_results(search_query)
        )

    def _yield_graphql_search_results(self, search_query: str) -> Generator[Dict, None, None]:
        total = self.MAX_SEARCH_RESULTS_TOTAL
        offset = 0
        while offset < total:
            graphql_variables = {
                'search_query': search_query,
                'page_size': self.PAGE_SIZE,
                'offset': offset
            }
            logger.info(f'Issuing graphql query with variables: {graphql_variables}')

            payload = {'query': self.GRAPHQL_QUERY, 'variables': graphql_variables}
            response = requests.post(self.graphql_api_url, json=payload)
            response.raise_for_status()
            response_json = response.json()
            for entity in response_json['data']['search']['searchResults']:
                yield entity

            total = response_json['data']['search']['total']
            if total > self.MAX_SEARCH_RESULTS_TOTAL:
                msg = f'graphql query total results ({total}) exceeds maximum ({self.MAX_SEARCH_RESULTS_TOTAL})'
                raise RuntimeError(msg)
            offset += self.PAGE_SIZE

    @classmethod
    def _yield_rows(cls, entity: Dict) -> Generator[Dict, None, None]:
        # Merge glossaryTerms from both schemaMetadata and editableSchemaMetadata
        merged_rows = {}
        # schemaMetadata will contain all fields
        for field in entity['entity']['schemaMetadata']['fields']:
            merged_rows[field['fieldPath']] = {
                'dataset': entity['entity']['schemaMetadata']['name'],
                'field': field['fieldPath'],
                'type': [],
                'privacy_law': [],
            }
            cls._add_terms_to_row(merged_rows[field['fieldPath']], field)

        # editableSchemaMetadata can be empty or contain subset of fields
        if entity['entity']['editableSchemaMetadata']:
            for field in entity['entity']['editableSchemaMetadata']['editableSchemaFieldInfo']:
                cls._add_terms_to_row(merged_rows[field['fieldPath']], field)

        yield from merged_rows.values()

    @classmethod
    def _add_terms_to_row(cls, row: Dict, field: Dict) -> None:
        if not field['glossaryTerms']:
            return
        for term in field['glossaryTerms']['terms']:
            term_id = term['term']['urn'].split(':')[-1]
            if term_id.startswith('PiiData'):
                row['type'].append(term_id.split('.')[-1])
            if term_id.startswith('PrivacyLaw'):
                row['privacy_law'].append(term_id.split('.')[-1])

class OutputWriter:
    """ Handles writing to a `fileobj` based on the `output_format`.

    - `write` will write to the `fileobj` passed in in constructor
    - `fileobj` should be managed outside of this class
    """
    output_format: OutputFormatEnum
    fileobj: TextIO

    def __init__(self, output_format: OutputFormatEnum, fileobj: TextIO):
        self.output_format = output_format
        self.fileobj = fileobj

    def write(self, rows: Iterable[Dict]):
        if self.output_format == OutputFormatEnum.CSV:
            self._write_csv(rows)
        else:
            raise ValueError(f'unhandled output format: {self.output_format}')

    def _write_csv(self, rows: Iterable[Dict]) -> None:
        fieldnames = ['dataset', 'field', 'type', 'privacy_law']
        writer = csv.DictWriter(self.fileobj, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            # TODO: make this more generic rather than just privacy
            # transform lists to be csv friendly
            row['type'] = ', '.join(sorted(row['type']))
            row['privacy_law'] = ', '.join(sorted(row['privacy_law']))
            writer.writerow(row)

class OutputExporter:
    """Use this as a context manager. Write using `fileobj` property and call `export()`.
    The `fileobj` will be closed on context manager exit.

    >>> with OutputExporter(destination) as exporter:
    >>>     exporter.fileobj.write(...)
    >>>     exporter.export()
    """

    output_destination: OutputDestination
    _fileobj: TextIO

    def __init__(self, output_destination: OutputDestination):
        self.output_destination = output_destination
        destination_type = type(self.output_destination)
        if destination_type == FileOutput:
            self._fileobj = open(output_destination.filepath, 'w')
        elif destination_type == S3Output:
            self._fileobj = tempfile.NamedTemporaryFile('w')
        else:
            raise ValueError(f'unhandled output destination type: {destination_type}')

    def __enter__(self) -> None:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self._fileobj.close()

    @property
    def fileobj(self) -> TextIO:
        return self._fileobj

    def export(self):
        if type(self.output_destination) == FileOutput:
            # File should be written to, nothing to export
            logger.info(f'Output at: {os.path.abspath(self._fileobj.name)}')
        elif type(self.output_destination) == S3Output:
            # set file to beginning before upload
            # or else it will upload an empty file.
            self.fileobj.seek(0)
            bucket = self.output_destination.s3_bucket
            key = datetime.datetime.utcnow().strftime(self.output_destination.s3_key)
            S3FileUploader().upload(self.fileobj.name, bucket, key)
        else:
            raise ValueError(f'unhandled output destination: {self.output_destination}')

class S3FileUploader:
    s3_client: Any

    def __init__(self, s3_client=None) -> None:
        self.s3_client = s3_client or boto3.Session().client('s3')

    def upload(self, src_filepath, s3_bucket, s3_key) -> None:
        logger.info(f'Uploading {src_filepath} to s3://{s3_bucket}/{s3_key}')
        self.s3_client.upload_file(src_filepath, s3_bucket, s3_key)
