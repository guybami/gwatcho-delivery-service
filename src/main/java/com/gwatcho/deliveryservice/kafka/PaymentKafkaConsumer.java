package com.gwatcho.deliveryservice.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gwatcho.deliveryservice.event.PaymentCompletedEvent;
import com.gwatcho.deliveryservice.service.DeliveryService;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import org.apache.kafka.common.serialization.StringDeserializer;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
@RequiredArgsConstructor
@Slf4j
public class PaymentKafkaConsumer {

    private final DeliveryService deliveryService;
    private final ObjectMapper objectMapper;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    @Value("${app.kafka.topics.payment-completed}")
    private String topic;

    private KafkaConsumer<String, String> consumer;

    private ExecutorService executor;

    private volatile boolean running;


    // =========================================================
    // START
    // =========================================================

    @PostConstruct
    public void start() {

        Properties properties =
                new Properties();

        properties.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServers
        );

        properties.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                groupId
        );

        properties.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName()
        );

        properties.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName()
        );

        properties.put(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest"
        );

        properties.put(
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                "false"
        );

        consumer =
                new KafkaConsumer<>(
                        properties
                );

        consumer.subscribe(
                List.of(topic)
        );

        running = true;

        executor = Executors.newSingleThreadExecutor();

        executor.submit(
                this::poll
        );

        log.info(
                "Kafka consumer started: topic={}, groupId={}",
                topic,
                groupId
        );
    }


    // =========================================================
    // POLL LOOP
    // =========================================================

    private void poll() {

        try {

            while (running) {
                var records =
                        consumer.poll(
                                Duration.ofMillis(1000)
                        );
                for (ConsumerRecord<String, String> record : records) {
                    process(record);
                }
            }
        } catch (Exception ex) {
            if (running) {
                log.error(
                        "Kafka consumer failed",
                        ex
                );
            }
        }
    }


    // =========================================================
    // PROCESS MESSAGE
    // =========================================================

    private void process(ConsumerRecord<String, String> record) {
        try {
            log.info(
                    "Received Kafka event: topic={}, partition={}, offset={}, key={}",
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    record.key()
            );


            PaymentCompletedEvent event =
                    objectMapper.readValue(
                            record.value(),
                            PaymentCompletedEvent.class
                    );


            log.info(
                    "PaymentCompletedEvent: paymentId={}, orderId={}, customerId={}, amount={}, currency={}, transactionId={}",
                    event.paymentId(),
                    event.orderId(),
                    event.customerId(),
                    event.amount(),
                    event.currency(),
                    event.transactionId()
            );


            deliveryService.handlePaymentCompleted(
                    event
            );


            consumer.commitSync();

            log.info(
                    "Kafka event processed successfully: orderId={}",
                    event.orderId()
            );

        } catch (Exception ex) {

            log.error(
                    "Failed to process Kafka event: topic={}, partition={}, offset={}",
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    ex
            );

            /*
             * IMPORTANT:
             * Do not commit the offset when processing fails.
             *
             * Kafka will deliver the message again.
             */
        }
    }


    // =========================================================
    // STOP
    // =========================================================

    @PreDestroy
    public void stop() {

        running = false;

        if (consumer != null) {
            consumer.wakeup();
        }

        if (executor != null) {
            executor.shutdownNow();
        }

        log.info(
                "Kafka consumer stopped"
        );
    }
}