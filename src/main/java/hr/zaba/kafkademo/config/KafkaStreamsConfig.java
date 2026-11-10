package hr.zaba.kafkademo.config;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;

import hr.zaba.kafkademo.dto.DriverLocation;
import hr.zaba.kafkademo.dto.Order;
import hr.zaba.kafkademo.dto.RideMatch;
import hr.zaba.kafkademo.dto.RideRequest;
import hr.zaba.kafkademo.dto.Transaction;
import lombok.extern.slf4j.Slf4j;
import java.util.Arrays;

@Slf4j
@Configuration
public class KafkaStreamsConfig {

    @Value(value = "${spring.kafka.bootstrap-servers}")
    private String bootstrapAddress;

    private static final String APP_ID = "testStreams";

    private static final double MAX_DISTANCE = 0.005;

    private static final Serde<String> STRING_SERDE = Serdes.String();
    private static final Serde<Double> DOUBLE_SERDE = Serdes.Double();

    private Serde<Order> orderSerde() {
        return Serdes.serdeFrom(new JacksonJsonSerializer<>(), new JacksonJsonDeserializer<>(Order.class));
    }

    private Serde<Transaction> transactionSerde() {
        return Serdes.serdeFrom(new JacksonJsonSerializer<>(), new JacksonJsonDeserializer<>(Transaction.class));
    }

    private Serde<DriverLocation> driverSerde() {
        return Serdes.serdeFrom(new JacksonJsonSerializer<>(), new JacksonJsonDeserializer<>(DriverLocation.class));
    }

    private Serde<RideRequest> rideSerde() {
        return Serdes.serdeFrom(new JacksonJsonSerializer<>(), new JacksonJsonDeserializer<>(RideRequest.class));
    }

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kStreamsConfig() {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, STRING_SERDE.getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, STRING_SERDE.getClass().getName());
        //props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);

        return new KafkaStreamsConfiguration(props);
    }

    @Bean
    @ConditionalOnExpression("false")
    public KStream<String, String> kStream(StreamsBuilder kStreamBuilder) {
        KStream<String, String> stream = kStreamBuilder.stream("my-input-streams-topic");
        KTable<String, Long> wordsCount = stream
                .peek((key, value) -> log.debug("ZABA Received: key=" + key + ", value=" + value))
                .mapValues((ValueMapper<String, String>) String::toLowerCase)
                .peek((key, value) -> log.debug("ZABA After mapValues: " + value))
                .flatMapValues(value -> Arrays.asList(value.split("\\W+")))
                .peek((key, value) -> log.debug("ZABA After flatMapValues: key: " + key + ", value " + value))
                .groupBy((key, value) -> value)
                .count();

        wordsCount.toStream().to("my-output-streams-topic", Produced.with(STRING_SERDE, Serdes.Long()));
        return stream;
    }

    @Bean
    @ConditionalOnExpression("false")
    public KStream<String, Order> ordersStream(StreamsBuilder kStreamBuilder) {
        KStream<String, Order> ordersStream = kStreamBuilder
        .stream("orders",
            Consumed.with(Serdes.String(),
            Serdes.serdeFrom(new JacksonJsonSerializer<>(), new JacksonJsonDeserializer<>(Order.class)))
        );

        // 1. Filter — only orders with amount > 100
        KStream<String, Order> filteredOrders = ordersStream
            .filter((key, value) -> value.getAmount() > 100);

        // 2. Map — label each passing order
        filteredOrders
            .mapValues(value -> "Processed Order: " + value)
            .to("processed-orders", Produced.with(STRING_SERDE, STRING_SERDE));

        // Test EXACTLY_ONCE_V2
        /*filteredOrders = ordersStream
            .filter((key, value) -> value.getAmount() > Double.valueOf("wee"));*/

        // 3. Aggregate — running total amount per order ID
        filteredOrders
            .mapValues(order -> order.getAmount())
            .peek((key, value) -> log.debug("After extracting amount: key=" + key + ", value=" + value))
            .groupByKey(Grouped.with(STRING_SERDE, DOUBLE_SERDE))
            .reduce(Double::sum)
            .toStream()
            .to("aggregated-orders", Produced.with(STRING_SERDE, DOUBLE_SERDE));

        return ordersStream;
    }

    @Bean
    @ConditionalOnExpression("false")
    public KStream<String, Order> joinStream(StreamsBuilder kStreamBuilder) {

        KStream<String, Order> leftStream = kStreamBuilder
            .stream("topic-a", Consumed.with(Serdes.String(), this.orderSerde()));
        KStream<String, Order> rightStream = kStreamBuilder
            .stream("topic-b", Consumed.with(Serdes.String(), this.orderSerde()));

        ValueJoiner<Order, Order, Order> valueJoiner = (leftValue, rightValue) -> {
            Order ret = new Order();
            ret.setCustomerId(leftValue.getCustomerId());
            ret.setAmount(leftValue.getAmount() + rightValue.getAmount());
            return ret;
        };

        KStream<String, Order> joinedStream = leftStream
        .peek((key, value) -> log.debug("Before join: key=" + key + ", value=" + value))
        .join(rightStream, 
            valueJoiner, 
            JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(10)),
            StreamJoined.with(
                Serdes.String(),
                this.orderSerde(),
                this.orderSerde()
            )
        );

        joinedStream.to("join-topic", Produced.with(STRING_SERDE, this.orderSerde()));

        return joinedStream;
    }

    @Bean
    @ConditionalOnExpression("false")
    public KStream<String, RideMatch> uberStream(StreamsBuilder kStreamBuilder) {
        KTable<String, DriverLocation> drivers = kStreamBuilder.table(
                "driver-topic",
                Consumed.with(Serdes.String(), driverSerde())
            );

        KStream<String, RideRequest> rides = kStreamBuilder.stream(
                "ride-topic",
                Consumed.with(Serdes.String(), rideSerde())
            );

        // Eventi se automatski uparuju po ključevima.
        KStream<String, RideMatch> matches = rides.join(
                drivers, (ride, driver) -> new RideMatch(ride, driver)
            );

        matches
            .filter((key, match) -> {

                double latDiff =
                    match.getRide().getLat() - match.getDriver().getLat();
                double lonDiff =
                    match.getRide().getLon() - match.getDriver().getLon();
                double distance =
                    Math.sqrt(latDiff * latDiff + lonDiff * lonDiff);

                return distance < MAX_DISTANCE;
            })
            .mapValues(RideMatch::getRide)
            .to(
                "active-requests",
                Produced.with(
                    Serdes.String(),
                    rideSerde()
                )
            );

        return matches;
    }


    @Bean
    @ConditionalOnExpression("true")
    public KStream<String, Transaction> transactionsStream(
            StreamsBuilder kStreamBuilder) {

        KStream<String, Transaction> transactionsStream =
            kStreamBuilder.stream(
                "transactions-input",
                Consumed.with(Serdes.String(), transactionSerde())
            );

        transactionsStream
            .peek((key, value) ->
                log.info("Received transaction: {}", value)
            )
            .split()
            .branch(
                (key, value) -> "APPROVED".equalsIgnoreCase(value.getStatus()),
                Branched.withConsumer(ks ->
                    ks.to("transactions-approved")
                )
            )
            .branch(
                (key, value) -> "REJECTED".equalsIgnoreCase(value.getStatus()),
                Branched.withConsumer(ks ->
                    ks.to("transactions-rejected")
                )
            )
            .branch(
                (key, value) -> "PENDING".equalsIgnoreCase(value.getStatus()),
                Branched.withConsumer(ks ->
                    ks.to("transactions-pending")
                )
            )
            .defaultBranch(
                Branched.withConsumer(ks ->
                    ks.to("transactions-unknown")
                )
            );

        return transactionsStream;
    }

}
