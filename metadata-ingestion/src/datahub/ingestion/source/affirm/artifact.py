import sys
import os
import logging
from dataclasses import dataclass, field
from typing import Iterable, Sequence
from ruamel.yaml import YAML

import datahub.emitter.mce_builder as builder
from datahub.configuration.source_common import PlatformSourceConfigBase
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


yaml = YAML(typ='rt')
logging.basicConfig(stream=sys.stderr, level=logging.INFO)


@dataclass
class AffirmArtifact:
    schema_name: str
    name: str
    description: str
    owner: str
    privacy_entrypoint: str
    retention_days: str
    processing_purposes: Sequence[str]

    def __post_init__(self):
        if self.privacy_entrypoint is None:
            self.privacy_entrypoint = ''
        if self.processing_purposes is None:
            self.processing_purposes = []
        if self.schema_name is None:
            self.schema_name = ''


class AffirmArtifactSourceConfig(PlatformSourceConfigBase):
    ''' TODO support git repo to automate the whole process: 
    git clone, locate artifact and ingest
    '''
    directory: str
    env: str


def iterate_artifact(directory: str) -> Iterable[AffirmArtifact]:
    def fix_description(description: str):
        return ' '.join(description.split())
    def get_schema(dir: str):
        relative_dir = dir.replace(os.path.abspath(directory), '').strip()
        relative_dir = relative_dir if not relative_dir.startswith(os.sep) else relative_dir[len(os.sep):]
        relative_dir = relative_dir if not relative_dir.endswith(os.sep) else relative_dir[len(os.sep):]
        return '.'.join(relative_dir.split(os.sep))
    for dir, _, filenames in os.walk(directory):
        for filename in filenames:
            if filename.endswith('.yaml'):
                filepath = os.path.join(dir, filename)
                with open(filepath, 'r') as f:
                    content = yaml.load(f)
                    artifact = AffirmArtifact(
                        schema_name=get_schema(dir),
                        name=filename.replace('.yaml', ''),
                        description=fix_description(content.get('description', '')),
                        owner=content.get('owner', ''),
                        privacy_entrypoint=content.get('privacy_entrypoint', ''),
                        retention_days=str(content.get('retention_days')) if content.get('retention_days') else '',
                        processing_purposes=content.get('processing_purposes', [])
                    )
                    yield artifact


def make_groupname(team: str) -> str:
    prefix = 'teams/'
    if team.startswith(prefix):
        team = team[len(prefix):]
    dot_delimited = ".".join(reversed(team.split('/')))
    camel_case = dot_delimited.replace('_', ' ').title().replace(' ', '')
    return f'{camel_case}.Team'

def make_processing_purpose_term(term: str) -> str:
    """
    processing_purposes.payment_processing -> ProcessingPurpose.PaymentProcessing
    """
    prefix = 'processing_purposes.'
    if term.startswith(prefix):
        term = term[len(prefix):]
    camel_case = term.replace('_', ' ').title().replace(' ', '')
    return f'ProcessingPurpose.{camel_case}'


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
        platform_instance = self.config.platform_instance
        env = self.config.env
        for artifact in iterate_artifact(directory):
            dataset_name = (
                f'{artifact.schema_name}.{artifact.name}'
                if len(artifact.schema_name) > 0
                else artifact.name
            )
            logging.info(f'> Processing dataset {dataset_name}')
            dataset_urn = builder.make_dataset_urn_with_platform_instance(
                platform=platform,
                name=dataset_name,
                platform_instance=platform_instance,
                env=env
            )
            dataset_snapshot = DatasetSnapshot(
                urn=dataset_urn,
                aspects=[],
            )
            # set up dataset properties 
            dataset_properties = DatasetPropertiesClass(
                description=artifact.description,
                tags=[],
                customProperties={
                    'privacy_entrypoint': artifact.privacy_entrypoint,
                    'retention_days': f'{artifact.retention_days}'
                }
            )
            dataset_snapshot.aspects.append(dataset_properties)
            # set up ownership
            ownership = OwnershipClass(
                owners=[
                    OwnerClass(
                        owner= builder.make_group_urn(make_groupname(artifact.owner)),
                        type=OwnershipTypeClass.DATAOWNER
                    )
                ]
            )
            dataset_snapshot.aspects.append(ownership)
            # set up processing purposes glossary terms
            processing_purposes = [make_processing_purpose_term(x) for x in artifact.processing_purposes]
            processing_purpose_urns = [builder.make_term_urn(x) for x in processing_purposes]
            processing_purpose_terms = builder.make_glossary_terms_aspect_from_urn_list(processing_purpose_urns)
            dataset_snapshot.aspects.append(processing_purpose_terms)
            # build mce & metadata work unit
            mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
            wu = MetadataWorkUnit(id=dataset_name, mce=mce)
            yield wu

    def get_report(self):
        return self.report

    def close(self):
        pass
