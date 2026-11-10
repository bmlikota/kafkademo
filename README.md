# Kafka Client

## Request

Slanje requesta na order post:

```sh
curl -X POST http://localhost:8081/kafka/order \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": 1,
    "product": "Laptop",
    "amount": 1200.0
  }'
```

## Streams API

WordCount producer:

```sh
docker exec --workdir /opt/kafka/ -it kafka sh \
bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic my-input-streams-topic
```

WordCount consumer:

```sh
docker exec --workdir //opt/kafka/ -it kafka sh \
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
--topic my-output-streams-topic \
--from-beginning \
--formatter-property print.key=true \
--formatter-property  print.value=true \
--formatter-property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
--formatter-property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
```
