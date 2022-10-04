import csv
import datetime
import itertools
import logging
import os
import re
import tempfile
from typing import Any, BinaryIO, Dict, Generator, Iterable, TextIO

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
        extractor = PrivacyTermExtractor(self.config.datahub_base_url, self.config.datahub_token)
        rows = extractor.yield_search_results(self.config.search_queries)
        with tempfile.NamedTemporaryFile("w") as tmp_file:
            output_writer = OutputWriter(self.config.output.format, tmp_file)
            output_writer.write(rows)
            with open(tmp_file.name, "rb") as tmp_file_binary:
                for destination in self.config.output.destinations:
                    tmp_file_binary.seek(0)
                    OutputExporter.export(destination, tmp_file_binary)
        return 0

    @classmethod
    def create(cls, config_dict: dict) -> "ReportGenerator":
        config = GenerateReportConfig.parse_obj(config_dict)
        return cls(config)


class PrivacyTermExtractor:
    DATAHUB_GRAPHQL_ENDPOINT: str = "/api/graphql"
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
    datahub_token: str
    search_query_page_size: int = 500

    def __init__(
        self, datahub_base_url: str, datahub_token: str, search_query_page_size: int = 500
    ) -> None:
        self.graphql_api_url = f"{datahub_base_url}{self.DATAHUB_GRAPHQL_ENDPOINT}"

        env_match = re.match("^\$\{(.*)\}$", datahub_token)
        if env_match:
            var = env_match.group(1)
            token = os.environ.get(var)
            if token:
                self.datahub_token = token
            else:
                raise ValueError(f"Environment variable {var} does not exist")
        else:
            self.datahub_token = datahub_token

        self.search_query_page_size = search_query_page_size

    def yield_search_results(
        self, search_queries: Iterable[str]
    ) -> Generator[Dict, None, None]:
        return itertools.chain.from_iterable(
            self._yield_rows(entity)
            for search_query in search_queries
            for entity in self._yield_graphql_search_results(search_query)
        )

    def _yield_graphql_search_results(
        self, search_query: str
    ) -> Generator[Dict, None, None]:
        total_num_of_datasets = None
        offset = 0
        while total_num_of_datasets is None or offset < total_num_of_datasets:
            graphql_variables = {
                "search_query": search_query,
                "page_size": self.search_query_page_size,
                "offset": offset,
            }
            logger.info(f"Issuing graphql query with variables: {graphql_variables}")

            headers = {"Authorization": f"Bearer {self.datahub_token}"}
            payload = {"query": self.GRAPHQL_QUERY, "variables": graphql_variables}
            response = requests.post(self.graphql_api_url, headers=headers, json=payload)
            response.raise_for_status()
            response_json = response.json()

            num_entities_processed_for_current_query = 0
            for entity in response_json["data"]["search"]["searchResults"]:
                yield entity
                num_entities_processed_for_current_query += 1

            total_num_of_datasets = response_json["data"]["search"]["total"]
            offset += num_entities_processed_for_current_query

    @classmethod
    def _yield_rows(cls, entity: Dict) -> Generator[Dict, None, None]:
        # Merge glossaryTerms from both schemaMetadata and editableSchemaMetadata
        merged_rows = {}
        # schemaMetadata can be empty due to the zombie issue. Let's not fail here.
        if entity["entity"]["schemaMetadata"]:
            # schemaMetadata will contain all fields
            for field in entity["entity"]["schemaMetadata"]["fields"]:
                merged_rows[field["fieldPath"]] = {
                    "dataset": entity["entity"]["schemaMetadata"]["name"],
                    "field": field["fieldPath"],
                    "type": [],
                    "privacy_law": [],
                }
                cls._add_terms_to_row(merged_rows[field["fieldPath"]], field)

        # editableSchemaMetadata can be empty or contain subset of fields
        if entity["entity"]["editableSchemaMetadata"]:
            for field in entity["entity"]["editableSchemaMetadata"][
                "editableSchemaFieldInfo"
            ]:
                if field["fieldPath"] not in merged_rows:
                    logger.warning('Found fieldPath in editableSchemaMetadata.fields '
                                   'but not in schemaMetadata.fields: '
                                   f'fieldPath={field["fieldPath"]} '
                                   f'dataset={entity["entity"]["schemaMetadata"]["name"]}')
                    continue
                cls._add_terms_to_row(merged_rows[field["fieldPath"]], field)

        yield from merged_rows.values()

    @classmethod
    def _add_terms_to_row(cls, row: Dict, field: Dict) -> None:
        if not field["glossaryTerms"]:
            return
        for term in field["glossaryTerms"]["terms"]:
            term_id = term["term"]["urn"].split(":")[-1]
            if term_id.startswith("PiiData"):
                row["type"].append(term_id.split(".")[-1])
            if term_id.startswith("PrivacyLaw"):
                row["privacy_law"].append(term_id.split(".")[-1])


class OutputWriter:
    """Handles writing to a `fileobj` based on the `output_format`.

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
            raise ValueError(f"unhandled output format: {self.output_format}")

    def _write_csv(self, rows: Iterable[Dict]) -> None:
        fieldnames = ["dataset", "field", "type", "privacy_law"]
        writer = csv.DictWriter(self.fileobj, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            # TODO: make this more generic rather than just privacy
            # transform lists to be csv friendly
            row["type"] = ", ".join(sorted(row["type"]))
            row["privacy_law"] = ", ".join(sorted(row["privacy_law"]))
            writer.writerow(row)


class OutputExporter:
    @staticmethod
    def export(output_destination: OutputDestination, fileobj: BinaryIO):
        destination_type = type(output_destination)
        if destination_type == FileOutput:
            logger.info(
                f"Writing output to: {os.path.abspath(output_destination.filepath)}"
            )
            with open(output_destination.filepath, "wb") as f:
                f.write(fileobj.read())
        elif destination_type == S3Output:
            bucket = output_destination.s3_bucket
            key = datetime.datetime.utcnow().strftime(output_destination.s3_key)
            S3FileUploader().upload_fileobj(fileobj, bucket, key)
        else:
            raise ValueError(f"unhandled output destination type: {destination_type}")


class S3FileUploader:
    s3_client: Any

    def __init__(self, s3_client=None) -> None:
        self.s3_client = s3_client or boto3.Session().client("s3")

    def upload_fileobj(self, fileobj: BinaryIO, s3_bucket, s3_key) -> None:
        logger.info(f"Uploading to s3://{s3_bucket}/{s3_key}")
        self.s3_client.upload_fileobj(fileobj, s3_bucket, s3_key)
