import json
import logging
from collections import OrderedDict
from typing import Dict, Iterable, List, Optional
from dataclasses import dataclass, field

from datahub.cli.cli_utils import get_aspects_for_entity
from datahub.configuration.source_common import PlatformInstanceConfigMixin
from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataset_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.aws.aws_common import AwsSourceConfig
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
)
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    ArrayType,
    BooleanType,
    BytesType,
    KeyValueSchema,
    MapType,
    NullType,
    NumberType,
    SchemaField,
    SchemaFieldDataType,
    SchemaMetadata,
    StringType,
)


logger = logging.getLogger(__name__)

DEFAULT_PLATFORM = "dynamodb"

_attribute_type_mapping = {
    "S": StringType,
    "N": NumberType,
    "B": BytesType,
    "SS": ArrayType,
    "NS": ArrayType,
    "BS": ArrayType,
    "M": MapType,
    "L": ArrayType,
    "BOOL": BooleanType,
    "NULL": NullType,
}


class DynamoDBSourceConfig(AwsSourceConfig, PlatformInstanceConfigMixin):
 
    ingest_tables: Optional[List[str]] = None
    s3_snapshot_schema_path: Optional[str] = None

    @property
    def dynamodb_client(self):
        return self.get_dynamodb_client()

    @property
    def dynamodbstreams_client(self):
        return self.get_dynamodbstreams_client()

    @property
    def s3_client(self):
        return self.get_s3_client()


@dataclass
class DynamoDBSourceReport(SourceReport):
    filtered_tables: List[str] = field(default_factory=list)
    streaming_disabled_tables: List[str] = field(default_factory=list)

    def report_table_filtered(self, table: str) -> None:
        self.filtered_tables.append(table)

    def report_table_streaming_disabled(self, table: str) -> None:
        self.streaming_disabled_tables.append(table)


@dataclass
class DynamoDBSource(Source):
    config: DynamoDBSourceConfig
    report: DynamoDBSourceReport()

    def __init__(self, config: DynamoDBSourceConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.config = config
        self.report = DynamoDBSourceReport()


    @classmethod
    def create(cls, source_config, context):
        config = DynamoDBSourceConfig.parse_obj(source_config)
        return cls(config, context)


    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        if self.config.ingest_tables:
            all_tables = self.config.ingest_tables
        else:
            all_tables = get_all_tables(self.config.dynamodb_client)
        for table_name in all_tables:
            logger.info(f"Processing table {table_name}")
            if not self.config.table_pattern.allowed(table_name):
                logger.info(f"Table {table_name} is not allowed by table_allow pattern")
                self.report.report_table_filtered(table_name)
                continue

            wu = self.make_workunit(table_name)
            if wu is None:
                logger.info(f'Skip none work unit of table {table_name}')
                continue
            yield wu


    def make_workunit(self, table_name: str):
        table = describe_table(self.config.dynamodb_client, table_name)
        keys = [x["AttributeName"] for x in table.get("KeySchema", [])]

        dataset_urn = make_dataset_urn(DEFAULT_PLATFORM, table_name, self.config.env)
        _aspects = get_aspects_for_entity(dataset_urn, [], typed=False)
        _existing_metadata = _aspects.get('schemaMetadata', {}).get('fields', {})
        existing_schema = {f['fieldPath']: f['nativeDataType'] for f in _existing_metadata}

        attribute_definitions = self.populate_attribute_definitions(table)
        if not attribute_definitions:
            return None
        # Merge the new schema with the exsiting one on Datahub
        attribute_definitions.update(existing_schema)
        schema_fields = self.get_schema_fields(attribute_definitions)
        schema_metadata = SchemaMetadata(
            schemaName=f"{table_name}",
            platform=make_data_platform_urn(DEFAULT_PLATFORM),
            version=0,
            hash="",
            platformSchema=KeyValueSchema(keySchema="", valueSchema=""),
            fields=schema_fields,
            primaryKeys=keys
        )

        mcp = MetadataChangeProposalWrapper(
            entityType="dataset",
            entityUrn=dataset_urn,
            changeType=ChangeTypeClass.UPSERT,
            aspectName="schemaMetadata",
            aspect=schema_metadata
        )
        wu = MetadataWorkUnit(
            id=dataset_urn,
            mcp=mcp
        )
        return wu


    def populate_attribute_definitions(self, table) -> Dict[str, str]:
        raw_attribute_definitions = table["AttributeDefinitions"]
        attribute_definitions = {
            x["AttributeName"]: x["AttributeType"] for x in raw_attribute_definitions
        }
        snapshot_attribute_definitions = self.populate_attribute_definitions_from_snapshot()
        if snapshot_attribute_definitions:
            attribute_definitions.update(snapshot_attribute_definitions)

        if table.get("StreamSpecification", {}).get("StreamEnabled", False):
            stream_arn = table["LatestStreamArn"]
            for attr, new_value in get_attribute_definitions(self.config.dynamodbstreams_client, stream_arn).items():
                existing_value = attribute_definitions.get(attr)
                if existing_value is not None and existing_value != new_value:
                    logger.warn(f'Overwriting {attr} type: was {existing_value} but now is {new_value}')
                attribute_definitions[attr] = new_value
        else:
            table_name = table["TableName"]
            logger.error(f"Table {table_name} streaming is NOT enabled.")
            self.report.report_table_streaming_disabled(table_name)
            raise RuntimeError(f"Table {table_name} streaming is NOT enabled.")

        return attribute_definitions


    def populate_attribute_definitions_from_snapshot(self) -> Optional[Dict[str, str]]:
        if self.config.s3_snapshot_schema_path is None:
            return None

        # TODO: use util function or add validation
        bucket, prefix = self.config.s3_snapshot_schema_path.lstrip('s3://').split('/', 1)
        logger.info(f"Looking for spark schema at {self.config.s3_snapshot_schema_path}")

        list_objects_response = self.config.s3_client.list_objects(Bucket=bucket, Prefix=prefix)
        json_file_keys = [
            obj['Key'] for obj in list_objects_response['Contents']
            if obj['Key'].endswith('.json')
        ]
        if not json_file_keys:
            logger.error(f"Found no spark schema json file at {self.config.s3_snapshot_schema_path}")
            return None

        # Assume there's only one JSON file containing spark schema.
        if len(json_file_keys) >= 1:
            logger.error(f"Found {len(json_file_keys)} json files at {self.config.s3_snapshot_schema_path}. "
                         "There should only be 1.")
            return None
        spark_json_schema_key = json_file_keys[0]
        logger.info(f"Found spark schema at {spark_json_schema_key}")

        object_response = self.config.s3_client.get_object(Bucket=bucket, Key=spark_json_schema_key)
        spark_schema_json = json.loads(object_response['Body'].read().decode('utf-8'))
        assert len(spark_schema_json['fields']) == 1
        assert spark_schema_json['fields'][0]['type']['type'] == 'struct'

        attribute_definitions = {}
        for field in spark_schema_json['fields'][0]['type']['fields']:
            field_name = field['name']
            assert field['type']['type'] == 'struct'
            assert len(field['type']['fields']) == 1

            dynamo_column_type_info = field['type']['fields'][0]
            attribute_definitions[field_name] = dynamo_column_type_info['name']

        logger.info(f"Spark schema is: {attribute_definitions}")
        return attribute_definitions


    def get_schema_fields(self, attribute_definitions: Dict):
        schema_fields = []
        for (k, v) in attribute_definitions.items():
            type_class = _attribute_type_mapping.get(v)
            schemaField = SchemaField(
                fieldPath=k,
                type=SchemaFieldDataType(type=type_class()),
                nativeDataType=v
            )
            schema_fields.append(schemaField)
        return schema_fields


    def get_report(self):
        return self.report


    def close(self):
        pass


def get_all_tables(dynamodb_client) -> List[str]:
    tables = []
    paginator = dynamodb_client.get_paginator("list_tables")
    paginator_iterator = paginator.paginate()
    for page in paginator_iterator:
        tables += page["TableNames"]
    return tables


def describe_table(dynamodb_client, table_name: str) -> Dict:
    response = dynamodb_client.describe_table(TableName=table_name)
    table = response["Table"]
    return table


def get_attribute_definitions(dynamodbstreams_client, arn: str) -> Dict:
    attribute_definitions = OrderedDict()
    description_response = dynamodbstreams_client.describe_stream(StreamArn=arn)
    description = description_response.get("StreamDescription", {})
    shards = description.get("Shards", [])

    for shard in shards:
        latest_sequence_number = ''
        completed = False
        logger.info(f"Processing shard: {shard['ShardId']}, stream arn: {arn}")
        while not completed:
            if 'EndingSequenceNumber' not in shard['SequenceNumberRange']:
                logger.info('The shard is still open, exiting')
                break

            call_kwargs = {
                'StreamArn': arn,
                'ShardId': shard["ShardId"],
                'ShardIteratorType': 'TRIM_HORIZON'
            }
            if latest_sequence_number:
                call_kwargs.update({
                    'SequenceNumber': latest_sequence_number,
                    'ShardIteratorType': 'AFTER_SEQUENCE_NUMBER'})

            try:
                shard_iterator_response = dynamodbstreams_client.get_shard_iterator(**call_kwargs)
            except dynamodbstreams_client.exceptions.ResourceNotFoundException:
                logger.warn(f'Shard is no longer available, id: {shard["ShardId"]}')
                break

            shard_iterator = shard_iterator_response["ShardIterator"]
            while shard_iterator is not None:
                try:
                    records_response = dynamodbstreams_client.get_records(ShardIterator=shard_iterator)
                except dynamodbstreams_client.exceptions.ExpiredIteratorException:
                    logger.info('Shard Iterator expired, refreshing')
                    break

                records = records_response.get('Records', [])
                if len(records) > 0:
                    dynamodb_data = [x.get('dynamodb', {}) for x in records]
                    latest_sequence_number = dynamodb_data[-1]['SequenceNumber']
                    new_images = [x.get('NewImage', {}) for x in dynamodb_data]
                    for new_image in new_images:
                        for key in new_image.keys():
                            if key not in attribute_definitions:
                                type_value = new_image[key]
                                t = list(type_value.keys())[0]
                                attribute_definitions[key] = t
                shard_iterator = records_response.get('NextShardIterator', None)
                if shard_iterator is None:
                    completed = True
    return attribute_definitions
