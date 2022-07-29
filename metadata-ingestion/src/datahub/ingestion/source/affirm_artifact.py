import sys
import os
import logging
from dataclasses import dataclass, field
from typing import Iterable
from ruamel.yaml import YAML

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
)


logging.basicConfig(stream=sys.stderr, level=logging.INFO)
yaml = YAML(typ='rt')


@dataclass
class AffirmArtifact:
    name: str
    description: str
    owner: str
    privacy_entrypoint: str
    retention_days: int

    def __post_init__(self):
        if self.privacy_entrypoint is None:
            self.privacy_entrypoint = ''
        if self.retention_days is None:
            self.retention_days = -1


class AffirmArtifactSourceConfig(ConfigModel):
    directory: str
    filename: str
    platform: str
    env: str


def iterate_artifact(directory: str) -> Iterable[AffirmArtifact]:
    for r, _, filenames in os.walk(directory):
        for filename in filenames:
            if filename.endswith('.yaml'):
                filepath = os.path.join(r, filename)
                with open(filepath, 'r') as f:
                    content = yaml.load(f)
                    artifact = AffirmArtifact(
                        name=filename.replace('.yaml', ''),
                        description=content.get('description', ''),
                        owner=content.get('owner', ''),
                        privacy_entrypoint=content.get('privacy_entrypoint', ''),
                        retention_days=content.get('retention_days', ''),
                    )
                    yield artifact


def make_groupname(team: str) -> str:
    prefix = "teams/"
    if team.startswith(prefix):
        team = team[len(prefix):]
    dot_delimited = ".".join(reversed(team.split("/")))
    camel_case = dot_delimited.replace("_", " ").title().replace(" ", "")
    return f'{camel_case}.Team'


@dataclass
class AffirmArtifactSource(Source):
    config: AffirmArtifactSourceConfig 
    report: SourceReport = field(default_factory=SourceReport)

    @classmethod
    def create(cls, config_dict, ctx):
        config = AffirmArtifactSourceConfig.parse_obj(config_dict)
        return cls(ctx, config)

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        directory = self.config.directory
        platform = self.config.platform
        env = self.config.env
        for artifact in iterate_artifact(directory):
            dataset_name = artifact.name
            logging.info(f'> Processing dataset {dataset_name}')
            dataset_urn = builder.make_dataset_urn(platform, dataset_name, env)
            dataset_snapshot = DatasetSnapshot(
                urn=dataset_urn,
                aspects=[],
            )
            dataset_properties = DatasetPropertiesClass(
                description=artifact.description,
                tags=[],
                customProperties={
                    'privacy_entrypoint': artifact.privacy_entrypoint,
                    'retention_days': f'{artifact.retention_days}'
                }
            )
            dataset_snapshot.aspects.append(dataset_properties)
            ownership = OwnershipClass(
                owners=[
                    OwnerClass(
                        owner= builder.make_group_urn(make_groupname(artifact.owner)),
                        type=OwnershipTypeClass.DATAOWNER
                    )
                ]
            )
            dataset_snapshot.aspects.append(ownership)
            mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
            wu = MetadataWorkUnit(id=dataset_name, mce=mce)
            yield wu

    def get_report(self):
        return self.report

    def close(self):
        pass
