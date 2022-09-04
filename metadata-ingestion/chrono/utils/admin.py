from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer


admin_client = AdminClient({
    "bootstrap.servers": "localhost:9092",
})
md = admin_client.list_topics()
print(md.topics.values())
print(len(md.topics.values()))

'''
schema_registry_config = {
    "url": "http://localhost:8081"
}
client = SchemaRegistryClient(schema_registry_config)
# client.delete_subject("AffirmChargesPricingFee-value")
schema = client.get_schema(1)
print(schema.name)
'''
