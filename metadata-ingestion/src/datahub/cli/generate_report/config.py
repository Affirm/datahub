from enum import Enum
from typing import List, Union

from datahub.configuration.common import ConfigModel


class OutputFormatEnum(str, Enum):
    CSV = 'csv'

class FileOutput(ConfigModel):
    filepath: str

class S3Output(ConfigModel):
    s3_bucket: str
    s3_key: str

OutputDestination = Union[FileOutput, S3Output]

class Output(ConfigModel):
    format: OutputFormatEnum
    destination: OutputDestination

class GenerateReportConfig(ConfigModel):
    datahub_base_url: str
    search_queries: List[str]
    output: Output
