package hr.zaba.kafkademo.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import hr.zaba.kafkademo.dto.Order;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class KafkaConsumerConfig {

    @Autowired
    private ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory;

    @PostConstruct
    public void checkAckMode() {
        log.info("Kafka AckMode: {}",
            kafkaListenerContainerFactory
                .getContainerProperties()
                .getAckMode());
        log.info("");
    }

    @KafkaListener(
        topics = { "mytopic" },
        groupId = "foo",
        concurrency = "1",
        properties = {
            ConsumerConfig.MAX_POLL_RECORDS_CONFIG + "=100"
    })
    public void listenWithHeaders(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) throws Exception {
       log.debug("Received Message: " + message
                + "\n from partition: " + partition + ", group: foo, offset: " + offset);

        //throw new RuntimeException("My exception");
    }

    @KafkaListener(
        topicPartitions = @TopicPartition(topic = "mytopic", partitionOffsets = { @PartitionOffset(partition = "0", initialOffset = "6") })
    )
    public void listenToPartition(
        @Payload String message,
        @Header(KafkaHeaders.RECEIVED_PARTITION) long partition,
        @Header(KafkaHeaders.OFFSET) int offset) {
            log.debug("Received Message: " + message + "\n from partition: " + partition + ", offset: " + offset);
    }


    @KafkaListener(
        topics = "orders",
        groupId = "orders-group",
        containerFactory = "ordersKafkaListenerContainerFactory"
    )
    public void listenOrders(Order order, @Header(KafkaHeaders.RECEIVED_PARTITION) int partition) {
        log.debug("Received order: " + order + " from partition: " + partition);
        // throw new RuntimeException("My exception");
    }

    @KafkaListener(
        topics = { "logins", "security-events", "payments" },
        groupId = "fraud-detection-group"
    )
    public void fraudDetection(String event, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        log.debug("Received order: " + event + " from topic: " + topic);
    }

}
