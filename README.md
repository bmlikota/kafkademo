# Guide

## Producer i consumer

```bash
# Kreiraj topic
docker exec --workdir //opt/kafka -it kafka \
bin/kafka-topics.sh --bootstrap-server localhost:9092 \
--create \
--replication-factor 1 \
--partitions 1 \
--topic mytopic

# Kreiraj consumera
docker exec --workdir //opt/kafka -it kafka \
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
--command-property group.protocol=consumer \
--topic mytopic --group grupa-a  
```

```sh
# Kreiranje producera:
docker exec --workdir //opt/kafka -it kafka sh \
bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic mytopic
```

```sh
# Citanje s particije
docker exec --workdir //opt/kafka -it kafka sh \
bin/kafka-console-consumer.sh \
--bootstrap-server localhost:9092 \
--topic mytopic \
--partition 0 \
--offset "5" # inicijalno procita sve offsete od 5 do kraja

# Kafka ne generira grupu za takvog consumera:
docker exec --workdir //opt/kafka -it kafka sh \
bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
```

Producer i consumer za `mytopic` topic:

```mermaid
---
title: stringStringKafkaTemplate implementira KafkaTemplate interface, ima metodu send
---
flowchart LR

    P[SpringProducer
      stringStringKafkaTemplate]

    subgraph Kafka Topic: mytopic
        P0["Partition 0
            events: e0,e1,e2"]
    end

    P -->|"e3"| P0

    subgraph foo
        Cons[SpringConsumer
          @KafkaListener]
        State["Committed Offsets<br/>P0=1"]
    end

    P0 -->|"e1"| Cons
    Cons --> State
```

```bash
# Kreiraj topic
docker exec --workdir //opt/kafka -i kafka \
bin/kafka-topics.sh --bootstrap-server localhost:9092 --create \
--replication-factor 1 \
--partitions 3 \
--topic orders

# Kreiraj consumera
docker exec --workdir //opt/kafka -it kafka sh \
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic orders --group grupa-b \
--command-property group.protocol=consumer \
--formatter-property print.key=true \
--formatter-property print.value=true \
--key-deserializer org.apache.kafka.common.serialization.LongDeserializer
```

```sh
# Slanje requesta na Rest endpoint
curl -X POST http://localhost:8082/kafka/order \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": 1,
    "product": "Laptop",
    "amount": 1200.0
  }'

# Ili preko terminala
# event 123:{ "customerId": "123", "product": "Laptop", "amount": 100 }
docker exec --workdir //opt/kafka -it kafka \
bin/kafka-console-producer.sh --topic orders \
--bootstrap-server localhost:9092 \
--reader-property "parse.key=true" \
--reader-property "key.separator=:"
```

Producer i consumer za `orders` topic.

```mermaid
---
title: longJsonKafkaTemplate implementira KafkaTemplate interface, ima metodu send
---
flowchart LR

    P[SpringProducer
      longJsonKafkaTemplate]

    subgraph Kafka Topic: orders
        P0["Partition 0
            events: e0,e1,e2"]
    end

    P -->|"e3"| P0

    subgraph orders-group
        Cons[SpringConsumer
          @KafkaListener]
        State["Committed Offsets<br/>P0=1"]
    end

    P0 -->|"e1"| Cons
    Cons --> State
```

Fraud detection topic:

```sh
docker exec --workdir //opt/kafka -it kafka sh \
bin/kafka-topics.sh --bootstrap-server localhost:9092 \
--create --topic logins
```

```sh
docker exec --workdir //opt/kafka -it kafka sh \
bin/kafka-topics.sh --bootstrap-server localhost:9092 \
--create --topic security-events
```

```sh
docker exec --workdir //opt/kafka -it kafka sh \
bin/kafka-topics.sh --bootstrap-server localhost:9092 \
--create --topic payments
```

Produceri:

```sh
docker exec --workdir //opt/kafka -it kafka sh \
bin/kafka-console-producer.sh --bootstrap-server localhost:9092 \
--topic logins
```

```sh
docker exec --workdir //opt/kafka -it kafka sh \
bin/kafka-console-producer.sh --bootstrap-server localhost:9092 \
--topic security-events
```

```sh
docker exec --workdir //opt/kafka -it kafka sh \
bin/kafka-console-producer.sh --bootstrap-server localhost:9092 \
--topic payments
```

Consumer za fraud detection sustav:

```mermaid
flowchart LR

    L[LoginService]
    S[SecurityService]
    P[PaymentService]

    subgraph Kafka Topic: logins
        L0["Partition 0
            events: e0,e1,e2,e3"]
    end

    subgraph Kafka Topic: security-events
        S0["Partition 0
            events: e0,e1,e2"]
    end

    subgraph Kafka Topic: payments
        P0["Partition 0
            events: e0,e1,e2,e3,e4"]
    end

    L -->|"UserLoggedIn(e4)"| L0
    S -->|"PasswordChanged(e3)"| S0
    P -->|"PaymentRequested(e5)"| P0

    subgraph fraud-detection-group
        FD[FraudDetectionConsumer 
          @KafkaListener]
        LOff["Logins Committed Offsets</br>P0=3"]
        SOff["Security Committed Offsets</br>P0=2"]
        POff["Payment Committed Offsets</br>P0=4"]
    end

    L0 -->|"e3"| FD
    S0 -->|"e2"| FD
    P0 -->|"e4"| FD

    FD --> LOff
    FD --> SOff
    FD --> POff
```

### Streams

#### WordCountDemo

```bash
# Kreiraj input topic
docker exec --workdir //opt/kafka/ -it kafka sh \
bin/kafka-topics.sh --bootstrap-server localhost:9092 \
--create \
--replication-factor 1 \
--partitions 1 \
--topic streams-plaintext-input

# Kreiraj output topic
docker exec --workdir //opt/kafka/ -it kafka sh \
bin/kafka-topics.sh --bootstrap-server localhost:9092 \
--create \
--replication-factor 1 \
--partitions 1 \
--topic streams-wordcount-output

# Vidi detalje kreiranih topic-a
docker exec --workdir //opt/kafka/ -it kafka sh \
bin/kafka-topics.sh --bootstrap-server localhost:9092 \
--describe --topic streams-wordcount-output
```

```bash
# Pokreni WordCount aplikaciju
docker exec --workdir //opt/kafka/ -it kafka sh \
bin/kafka-run-class.sh org.apache.kafka.streams.examples.wordcount.WordCountDemo

# Pokreni producer za input topic
docker exec --workdir //opt/kafka/ -it kafka sh \
bin/kafka-console-producer.sh --bootstrap-server localhost:9092 \
--topic streams-plaintext-input

# Pokreni consumer-a
docker exec --workdir //opt/kafka/ -it kafka sh \
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
--command-property group.protocol=consumer \
--topic streams-wordcount-output \
--from-beginning \
--formatter-property print.key=true \
--formatter-property print.value=true \
--formatter-property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
--formatter-property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
```

```mermaid
flowchart LR

    subgraph Input["Kafka Topic: streams-plaintext-input"]
        I0["Partition 0
            events: e0,e1"]
        I1["Partition 1
            events: e0,e1,e2"]
    end

    subgraph Processor["WordCount Stream Processor"]
        WC["WordCount Processor
            Čita rečenice
            Razdvaja ih na riječi
            Ažurira broj pojavljivanja
            svake riječi"]

        State["WordCount State Store
            hello = 3
            kafka = 2
            streams = 4"]

        WC -->|"Ažurira brojanje"| State
    end

    subgraph Output["Kafka Topic: streams-wordcount-output"]
        O0["Partition 0
            events:
            hello → 3
            kafka → 2
            streams → 4"]
    end

    I0 -->|"Sentence events"| WC
    I1 -->|"Sentence events"| WC

    State -->|"Trenutni broj riječi"| O0
```

#### WordCount SpringBoot implementacija

```bash
# Kreiraj input streams topic
docker exec --workdir //opt/kafka -it kafka \
bin/kafka-topics.sh --bootstrap-server localhost:9092 --create \
--replication-factor 1 \
--partitions 1 \
--topic my-input-streams-topic

# Kreiraj output streams topic
docker exec --workdir //opt/kafka -it kafka \
bin/kafka-topics.sh --bootstrap-server localhost:9092 --create \
--replication-factor 1 \
--partitions 1 \
--topic my-output-streams-topic

# Kreiraj producera
docker exec --workdir //opt/kafka -it kafka \
bin/kafka-console-producer.sh --topic my-input-streams-topic \
--bootstrap-server localhost:9092

# Pokreni consumer-a
docker exec --workdir //opt/kafka/ -it kafka sh \
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
--command-property group.protocol=consumer \
--topic my-output-streams-topic \
--from-beginning \
--formatter-property print.key=true \
--formatter-property print.value=true \
--formatter-property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
--formatter-property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
```

#### Orders procesor

```bash
# Kreiraj output streams topic
docker exec --workdir //opt/kafka -it kafka \
bin/kafka-topics.sh --bootstrap-server localhost:9092 --create \
--replication-factor 1 \
--partitions 1 \
--topic processed-orders

docker exec --workdir //opt/kafka -it kafka \
bin/kafka-topics.sh --bootstrap-server localhost:9092 --create \
--replication-factor 1 \
--partitions 1 \
--topic aggregated-orders
```

```sh
# Slanje requesta na Rest endpoint
curl -X POST http://localhost:8082/kafka/order \
  -H "Content-Type: application/json" \
  -d '{"customerId": 1, "product": "Laptop", "amount": 1200.0}'

# Consumer
docker exec --workdir //opt/kafka/ -it kafka sh \
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
--command-property group.protocol=consumer \
--topic aggregated-orders \
--from-beginning \
--formatter-property print.key=true \
--formatter-property  print.value=true \
--formatter-property key.deserializer=org.apache.kafka.common.serialization.LongDeserializer \
--formatter-property value.deserializer=org.apache.kafka.common.serialization.DoubleDeserializer
```

```mermaid
---
title: Orders Stream Processor - filtriranje, transformacija i agregacija narudžbi.
---
flowchart LR

    subgraph Input["Kafka Topic: orders"]
        I0["Partition 0
            events: e0,e1,e2,e3"]
        I1["Partition 1
            events: e0,e1,e2"]
    end

    subgraph Processor["Orders Stream Processor"]

        Filter["1. Filter
            amount > 100"]

        Map["2. MapValues
            Order → String
            Processed Order: Order"]

        Extract["3. MapValues
            Order → amount"]

        Group["GroupByKey
            Grupiranje prema
            customerId"]

        Reduce["Reduce
            Double::sum
            Kumulativni zbroj
            po customerId"]

        Filter -->|"Orders amount > 100"| Map
        Filter -->|"Orders amount > 100"| Extract
        Extract --> Group
        Group --> Reduce
    end

    subgraph Processed["Kafka Topic: processed-orders"]
        P0["Partition 0
            Processed Order: e0
            Processed Order: e2"]
    end

    subgraph Aggregated["Kafka Topic: aggregated-orders"]
        A0["Partition 0
            customer-1 → 350.0
            customer-2 → 250.0"]
    end

    I0 -->|"Order events"| Filter
    I1 -->|"Order events"| Filter

    Map -->|"Processed Order events"| P0
    Reduce -->|"Updated aggregate per customerId"| A0
```

#### Join streams

```sh
# kreiranje topica
docker exec --workdir //opt/kafka -it kafka \
bin/kafka-topics.sh --bootstrap-server localhost:9092 --create \
--replication-factor 1 \
--partitions 1 \
--topic topic-a

docker exec --workdir //opt/kafka -it kafka \
bin/kafka-topics.sh --bootstrap-server localhost:9092 --create \
--replication-factor 1 \
--partitions 1 \
--topic topic-b

docker exec --workdir //opt/kafka -it kafka \
bin/kafka-topics.sh --bootstrap-server localhost:9092 --create \
--replication-factor 1 \
--partitions 1 \
--topic join-topic
```

```sh
# event 123:{ "customerId": "123", "product": "Laptop", "amount": 300 }
# event 123:{ "customerId": "123", "product": "Laptop", "amount": 400 }
# event 123:{ "customerId": "123", "product": "Laptop", "amount": 100 }
# produceri

docker exec --workdir //opt/kafka -it kafka \
bin/kafka-console-producer.sh --topic topic-a \
--bootstrap-server localhost:9092 \
--reader-property "parse.key=true" \
--reader-property "key.separator=:"

docker exec --workdir //opt/kafka -it kafka \
bin/kafka-console-producer.sh --topic topic-b \
--bootstrap-server localhost:9092 \
--reader-property "parse.key=true" \
--reader-property "key.separator=:"
```

```mermaid
---
title: Join Stream Processor - spajanje eventa iz dva Kafka topica.
---
flowchart LR

    subgraph TopicA["Kafka Topic: topic-a"]
        A0["Partition 0
            events: a0,a1,a2"]
        A1["Partition 1
            events: a0,a1,a2"]
    end

    subgraph TopicB["Kafka Topic: topic-b"]
        B0["Partition 0
            events: b0,b1,b2"]
        B1["Partition 1
            events: b0,b1,b2"]
    end

    subgraph Processor["Join Stream Processor"]

        Left["Left KStream
            topic-a"]

        Right["Right KStream
            topic-b"]

        Join["Windowed Join
            Join po key-u
            Vremenski prozor: 10 sekundi"]

        Joiner["ValueJoiner
            customerId = left.customerId
            amount = left.amount + right.amount"]

        Left --> Join
        Right --> Join
        Join --> Joiner
    end

    subgraph Output["Kafka Topic: join-topic"]
        O0["Partition 0
            Joined Order events"]
        O1["Partition 1
            Joined Order events"]
    end

    A0 -->|"Order events"| Left
    A1 -->|"Order events"| Left

    B0 -->|"Order events"| Right
    B1 -->|"Order events"| Right

    Joiner -->|"Joined Order events"| O0
    Joiner -->|"Joined Order events"| O1
```

#### Uber

```sh
docker exec --workdir //opt/kafka -it kafka \
bin/kafka-topics.sh --bootstrap-server localhost:9092 --create \
--replication-factor 1 \
--partitions 1 \
--topic driver-topic

docker exec --workdir //opt/kafka -it kafka \
bin/kafka-topics.sh --bootstrap-server localhost:9092 --create \
--replication-factor 1 \
--partitions 1 \
--topic ride-topic

docker exec --workdir //opt/kafka -it kafka \
bin/kafka-topics.sh --bootstrap-server localhost:9092 --create \
--replication-factor 1 \
--partitions 1 \
--topic active-requests

docker exec --workdir //opt/kafka -it kafka \
bin/kafka-console-producer.sh --topic driver-topic \
--bootstrap-server localhost:9092 \
--reader-property "parse.key=true" \
--reader-property "key.separator=:"

docker exec --workdir //opt/kafka -it kafka \
bin/kafka-console-producer.sh --topic ride-topic \
--bootstrap-server localhost:9092 \
--reader-property "parse.key=true" \
--reader-property "key.separator=:"

# d-2:{ "driverId": "d-2", "lat": 45.81, "lon": 44.97}
# d-2:{ "rideId": "r-1", "driverId": "d-2", "lat": 45.8105, "lon": 55.97027 }
```

#### Branching

```sh
docker exec --workdir //opt/kafka -it kafka \
bin/kafka-topics.sh --bootstrap-server localhost:9092 --create \
--replication-factor 1 \
--partitions 1 \
--topic transactions-input

docker exec --workdir //opt/kafka -it kafka \
bin/kafka-topics.sh --bootstrap-server localhost:9092 --create \
--replication-factor 1 \
--partitions 1 \
--topic transactions-approved

docker exec --workdir //opt/kafka -it kafka \
bin/kafka-topics.sh --bootstrap-server localhost:9092 --create \
--replication-factor 1 \
--partitions 1 \
--topic transactions-rejected

docker exec --workdir //opt/kafka -it kafka \
bin/kafka-topics.sh --bootstrap-server localhost:9092 --create \
--replication-factor 1 \
--partitions 1 \
--topic transactions-pending

docker exec --workdir //opt/kafka -it kafka \
bin/kafka-topics.sh --bootstrap-server localhost:9092 --create \
--replication-factor 1 \
--partitions 1 \
--topic transactions-unknown

# { "transactionId": "tx-1001", "accountId": "acc-55", "amount": 250.75, "status": "PENDING" }
# { "transactionId": "tx-1002", "accountId": "acc-55", "amount": 20.75, "status": "REJECTED" }

docker exec --workdir //opt/kafka -it kafka \
bin/kafka-console-producer.sh --topic transactions-input \
--bootstrap-server localhost:9092
```

## Kafka Connect API

Kopirati config datoteke lokalno:

```sh
docker cp kafka:/opt/kafka/config .
```

Stop postojećeg servera:

```sh
docker compose down -v
```

Dodati u docker-compose.yaml:

```yaml
volumes:
  - ~/dev/Edukacije/KafkaRadionica:/home/appuser/shared:rw
  - ~/dev/Edukacije/KafkaRadionica/config:/opt/kafka/config:rw
```

Ponovo pokreni Kafka servis:

```sh
docker compose up -d 
```

Uđemo u container:

```sh
docker exec --workdir //opt/kafka -it kafka sh
```

Konfiguriranje konektora:

U config/connect-standalone.properties dodati:

```txt
plugin.path=libs/connect-file-4.3.1.jar
```

U config/connect-file-source.properties postaviti file putanju:

```txt
file=/home/appuser/shared/test.txt
```

U config/connect-file-sink.properties postaviti file putanju:

```txt
file=/home/appuser/shared/test.sink.txt
```

Pokretanje konektora:

```bash
# Pokreni connectore
docker exec --workdir //opt/kafka -it kafka sh \
bin/connect-standalone.sh config/connect-standalone.properties config/connect-file-source.properties config/connect-file-sink.properties
```

```sh
docker exec --workdir //opt/kafka -it kafka sh \
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
--topic connect-test --group test-group \
--command-property group.protocol=consumer
```

```mermaid
flowchart LR

    Source[("test.txt")]

    Import["ImportConnector"]

    subgraph Kafka Topic: connect-test
        P0["Partition 0
        events: row0,row1,row2"]
    end

    Export["ExportConnector"]

    Sink[("test.sink.txt")]

    Source -->|"Read rows"| Import

    Import -->|"Events"| P0

    P0 -->|"Events"| Export

    Export -->|"Write rows"| Sink
```

### Zadatak

Obriši sadržaj test.sink.txt datoteke.

```sh
# Kreiraj input streams topic
docker exec --workdir //opt/kafka -it kafka \
bin/kafka-topics.sh --bootstrap-server localhost:9092 --create \
--replication-factor 1 \
--partitions 1 \
--topic my-input-streams-topic

# Kreiraj output streams topic
docker exec --workdir //opt/kafka -it kafka \
bin/kafka-topics.sh --bootstrap-server localhost:9092 --create \
--replication-factor 1 \
--partitions 1 \
--topic my-output-streams-topic
```

U connect-file-source.properties staviti:

```txt
topic=my-input-streams-topic
```

U connect-file-sink.properties staviti:

```txt
topics=my-output-streams-topic
key.converter=org.apache.kafka.connect.storage.StringConverter
value.converter=org.apache.kafka.connect.converters.LongConverter
```

U config/connect-standalone.properties postaviti:

```txt
key.converter=org.apache.kafka.connect.storage.StringConverter
value.converter=org.apache.kafka.connect.storage.StringConverter
key.converter.schemas.enable=false
value.converter.schemas.enable=false
```

Schemas.enable=true očekuje: { "schema": ..., "payload": ...}
A schemas.enable=false očekuje čisti json: {"id":3,"ime":"Marko"}

```mermaid
flowchart TB

    Source[("test.txt")]

    Import["ImportConnector"]

    subgraph Kafka Topic: my-input-streams-topic
        P0["Partition 0
        events: row0,row1,row2"]
    end

    WordCounter["WordCounter
    (Stream Processor)"]

    subgraph Kafka Topic: my-output-streams-topic
        P1["Partition 0
        events: result0,result1,result2"]
    end

    Export["ExportConnector"]

    Sink[("test.sink.txt")]

    Source -->|"Read rows"| Import

    Import -->|"Events"| P0

    P0 -->|"Events"| WordCounter

    WordCounter -->|"Events"| P1

    P1 -->|"Events"| Export

    Export -->|"Write rows"| Sink
```
