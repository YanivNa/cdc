# cdc

Instructions:
- Run Zookeeper and Kafka servers.
- Start Spark by running SparkConsumer class.
- Publish messages to Kafka topic named "my-cdc-input-topic" by running class KafkaMain.
- 4 Messages will get published to the input topic: insert_message.json, update_message.json, delete_message.json and bad_message.json (taken from resources folder)
- Spark will consume the messages from the topic and will publish the relevant messages to output topic named "my-cdc-output-topic".


