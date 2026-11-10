package hr.zaba.kafkademo.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

import hr.zaba.kafkademo.dto.Order;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Configuration
public class KafkaConfig {

    @Value(value = "${spring.kafka.bootstrap-servers}")
    private String bootstrapAddress;

    // String producer
    @Bean
    public ProducerFactory<String, String> stringStringProducerFactory() {
        Map<String, Object> config = new HashMap<>();

        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapAddress);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    public KafkaTemplate<String, String> stringStringKafkaTemplate() {
        return new KafkaTemplate<>(stringStringProducerFactory());
    }

    // Order producer
    @Bean
    public ProducerFactory<Long, Order> longJsonProducerFactory() {
        Map<String, Object> config = new HashMap<>();

        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapAddress);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JacksonJsonSerializer.class);

        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    public KafkaTemplate<Long, Order> longJsonKafkaTemplate() {
        return new KafkaTemplate<>(longJsonProducerFactory());
    }

    // Order consumer
    //@Bean // ako ovo otkomentiramo, onda ce to biti handler za sve KafkaListenere 
    public DefaultErrorHandler ordersErrorHandler() {

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
            (record, exception) -> {
                // Logika u slucaju greske
                log.error(
                    "Failed to process Kafka message. Topic: {}, Key: {}, Error: {}",
                    record.topic(),
                    record.key(),
                    exception.getMessage(),
                    exception
                );

                // Mozemo spremiti neuspjeli event u bazu,You can save the failed message to a database,
                // poslati alert i/ili poslati na drugi topic
            },
            new FixedBackOff(1000L, 2L) // pokusaj 2 puta svako 1 sec, onda izvrsi gornju logiku ako ne uspije
        );

        // U slucaju ove greske nemoj pokusati ponovo
        errorHandler.addNotRetryableExceptions(
            IllegalArgumentException.class
        );

        return errorHandler;
    }

    @Bean
    public ConsumerFactory<Long, Order> orderConsumerFactory() {
        Map<String, Object> config = new HashMap<>();

        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapAddress);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "orders-group");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JacksonJsonDeserializer.class);

        config.put(JacksonJsonDeserializer.TRUSTED_PACKAGES, "*");

        return new DefaultKafkaConsumerFactory<>(config);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<Long, Order> ordersKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<Long, Order> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(orderConsumerFactory());
        factory.setCommonErrorHandler(ordersErrorHandler());
        return factory;
    }
}
