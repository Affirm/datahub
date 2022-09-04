from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema, SchemaReference
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer

import time

# topic = "AffirmChargesPricingFee"

topic = "user"

'''
# create topic in kafka
topics = [NewTopic(f"{topic}", 1, 1)]
admin_client = AdminClient({
    "bootstrap.servers": "localhost:9092",
})
admin_client.create_topics(topics)


# register schema into schema-registry
with open(f"./protobuf/{topic}.proto", "r") as f:
    schema_str = f.read()
schema_registry_config = {
    "url": "http://localhost:8081"
}
client = SchemaRegistryClient(schema_registry_config)
schema = Schema(
    schema_str,
    "PROTOBUF",
    # [SchemaReference(name="common_idol/extensions.proto", subject="common_idol/extensions.proto", version="1")]
)
client.register_schema(f"{topic}-key", schema)
'''


topics = [NewTopic("AffirmUnderwritesUser", 1, 1)]
admin_client = AdminClient({
     "bootstrap.servers": "localhost:9092",
})
result = admin_client.create_topics(topics)
print(result)

time.sleep(3)
