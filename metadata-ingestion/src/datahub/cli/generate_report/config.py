from enum import Enum
from typing import List, Union

from datahub.configuration.common import ConfigModel


class OutputFormatEnum(str, Enum):
    CSV = "csv"


class FileOutput(ConfigModel):
    filepath: str


class S3Output(ConfigModel):
    s3_bucket: str
    # The key can include strftime format codes and it will be formated
    # https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes
    s3_key: str


OutputDestination = Union[FileOutput, S3Output]


class Output(ConfigModel):
    format: OutputFormatEnum
    # Outputs can be exported to multiple destinations
    destinations: List[OutputDestination]


class GenerateReportConfig(ConfigModel):
    datahub_base_url: str
    # Search query strings for https://datahubproject.io/docs/graphql/queries#search
    search_queries: List[str]
    # Page size to use for the graphql query when fetching search results
    search_query_page_size: int = 500
    output: Output
