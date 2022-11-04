from typing import IO, Dict, List, Type, Union

import ujson

from datahub.ingestion.source.schema_inference.base import SchemaInferenceBase
from datahub.ingestion.source.schema_inference.object import construct_schema
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    ArrayTypeClass,
    BooleanTypeClass,
    NullTypeClass,
    NumberTypeClass,
    RecordTypeClass,
    SchemaField,
    SchemaFieldDataType,
    StringTypeClass,
    UnionTypeClass,
)

_field_type_mapping: Dict[Union[Type, str], Type] = {
    list: ArrayTypeClass,
    bool: BooleanTypeClass,
    type(None): NullTypeClass,
    int: NumberTypeClass,
    float: NumberTypeClass,
    str: StringTypeClass,
    dict: RecordTypeClass,
    "mixed": UnionTypeClass,
}


class JsonInferrer(SchemaInferenceBase):
    def merge_new_dictionary(self, complete_dict, new_dict):
        for key in new_dict.keys():
            if key not in complete_dict.keys():
                complete_dict[key] = new_dict[key]

        return complete_dict


    def parse_json(self, file):
        complete_datastore = {}
        for line in file:
            datastore = ujson.loads(line.decode('utf8'))
            complete_datastore = self.merge_new_dictionary(complete_datastore, datastore)

        return complete_datastore

    def infer_schema(self, file: IO[bytes]) -> List[SchemaField]:
        datastore = self.parse_json(file)

        if not isinstance(datastore, list):
            datastore = [datastore]

        schema = construct_schema(datastore, delimiter=".")
        fields: List[SchemaField] = []

        for schema_field in sorted(schema.values(), key=lambda x: x["delimited_name"]):
            mapped_type = _field_type_mapping.get(schema_field["type"], NullTypeClass)

            native_type = schema_field["type"]

            if isinstance(native_type, type):
                native_type = native_type.__name__

            field = SchemaField(
                fieldPath=schema_field["delimited_name"],
                nativeDataType=native_type,
                type=SchemaFieldDataType(type=mapped_type()),
                nullable=schema_field["nullable"],
                recursive=False,
            )
            fields.append(field)

        return fields
