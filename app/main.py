from ast import Compare
from re import S
import faust
from config import settings
from schema_registry.client import SchemaRegistryClient
from schema_registry.serializers.faust import FaustSerializer


app = faust.App(
    settings.APP_NAME,
    broker=settings.KAFKA_BROKER_URL
    )

registry_schema_client = SchemaRegistryClient(url=settings.SCHEMA_REGISTRY_URL)


metric_schema_version = registry_schema_client.get_schema("{}-value".format(settings.METRIC_TOPIC))
metric_serializer = FaustSerializer(registry_schema_client, "-value".format(settings.METRIC_TOPIC) , metric_schema_version.schema)

threshold_schema_version = registry_schema_client.get_schema("{}-value".format(settings.THRESHOLD_TOPIC))
threshold_serializer = FaustSerializer(registry_schema_client, "-value".format(settings.THRESHOLD_TOPIC) , threshold_schema_version.schema)

output_schema_version = registry_schema_client.get_schema("{}-value".format(settings.OUTPUT_TOPIC))
output_serializer = FaustSerializer(registry_schema_client, "-value".format(settings.OUTPUT_TOPIC) , output_schema_version.schema)

faust_serializer_output = 'avro_{}'.format(settings.OUTPUT_TOPIC)
faust.serializers.codecs.register(name=faust_serializer_output, codec=output_serializer)

class OutputTopic(faust.Record, serializer=faust_serializer_output):
    Threshold: str
    Metric: str
    MetricValue: float
    ThresholdValue: float
    Comparison: str




threshold_table = app.Table('threshold_table', default=dict, partitions=settings.PARTITIONS)


metric_topic = app.topic(settings.METRIC_TOPIC, value_serializer='raw')
threshold_topic = app.topic(settings.THRESHOLD_TOPIC, value_serializer='raw')

output_topic = app.topic(settings.OUTPUT_TOPIC, value_type=OutputTopic, key_type=str)

@app.agent(metric_topic)
async def process_metric(messages):
    async for message in messages:
        data = metric_serializer.message_serializer.decode_message(message)
        data = {k.lower(): v for k, v in data.items()}
        key = "{}_{}".format(data.get("Server", ""), data.get("Login", ""))
        metric = data.get(settings.METRIC_VALUE, -1)

        if metric != -1:
            table_data = threshold_table[key]
            table_data["metric"] = metric
            for index, value in table_data:
                if index == "metric":
                    continue
                if value.get("comparison", "") == "greater_or_equal":
                    if value.get("value", -1) >= metric :
                        
                        pass
                    continue
                if value.get("comparison", "") == "greater":
                    continue
                if value.get("comparison", "") == "smaller_or_equal":
                    continue
                if value.get("comparison", "") == "smaller":
                    continue
            


@app.agent(threshold_topic)
async def process_threshold(messages):
    async for message in messages:
        data = threshold_serializer.message_serializer.decode_message(message)
        data = {k.lower(): v for k, v in data.items()}
        key = data.get("key", "_")
        threshold = data.get("threshold", 0)


        table_data = threshold_table[key]
        table_data[threshold] = data
        threshold_table[key] = table_data

        