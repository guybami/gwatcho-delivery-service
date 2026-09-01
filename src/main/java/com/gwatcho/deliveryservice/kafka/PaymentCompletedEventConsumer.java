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
import org.apache.kafka.clients.consumer.OffsetAndMetadata;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import org.apache.kafka.common.serialization.StringDeserializer;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
@RequiredArgsConstructor
@Slf4j
public class PaymentCompletedEventConsumer {

    private final DeliveryService deliveryService;
    private final ObjectMapper objectMapper;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    @Value("${app.kafka.topics.payment-completed}")
    private String paymentCompletedTopic;

    private KafkaConsumer<String, String> kafkaConsumer;

    private ExecutorService executorService;

    private volatile boolean running;


    // =========================================================
    // START
    // =========================================================

    @PostConstruct
    public void start() {

        Properties properties = new Properties();

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

        kafkaConsumer =
                new KafkaConsumer<>(properties);

        kafkaConsumer.subscribe(
                List.of(paymentCompletedTopic)
        );

        running = true;

        executorService =
                Executors.newSingleThreadExecutor();

        executorService.submit(
                this::pollLoop
        );

        log.info(
                "PaymentCompletedEventConsumer started: " +
                        "bootstrapServers={}, topic={}, groupId={}",
                bootstrapServers,
                paymentCompletedTopic,
                groupId
        );
    }


    // =========================================================
    // POLL LOOP
    // =========================================================

    private void pollLoop() {

        try {

            while (running) {

                var records =
                        kafkaConsumer.poll(
                                Duration.ofSeconds(1)
                        );

                for (
                        ConsumerRecord<String, String> record
                        : records
                ) {

                    processRecord(record);
                }
            }

        } catch (WakeupException ex) {

            /*
             * WakeupException is expected when stop()
             * calls kafkaConsumer.wakeup().
             */
            if (running) {

                log.error(
                        "Kafka consumer interrupted unexpectedly",
                        ex
                );
            }

        } catch (Exception ex) {

            log.error(
                    "Fatal error in PaymentCompletedEventConsumer",
                    ex
            );

        } finally {

            closeConsumer();
        }
    }


    // =========================================================
    // PROCESS RECORD
    // =========================================================

    void processRecord(
            ConsumerRecord<String, String> record) {

        log.info(
                "Received payment.completed: " +
                        "topic={}, partition={}, offset={}, key={}",
                record.topic(),
                record.partition(),
                record.offset(),
                record.key()
        );


        try {

            PaymentCompletedEvent event =
                    objectMapper.readValue(
                            record.value(),
                            PaymentCompletedEvent.class
                    );


            validateEvent(event);


            log.info(
                    "PaymentCompletedEvent received: " +
                            "paymentId={}, orderId={}, customerId={}, " +
                            "amount={}, currency={}, transactionId={}",
                    event.paymentId(),
                    event.orderId(),
                    event.customerId(),
                    event.amount(),
                    event.currency(),
                    event.transactionId()
            );


            // -------------------------------------------------
            // Business logic
            // -------------------------------------------------

            deliveryService.handlePaymentCompleted(
                    event
            );


            // -------------------------------------------------
            // Commit ONLY after successful processing
            // -------------------------------------------------

            commitOffset(record);


            log.info(
                    "payment.completed processed successfully: " +
                            "orderId={}, partition={}, offset={}",
                    event.orderId(),
                    record.partition(),
                    record.offset()
            );

        } catch (Exception ex) {

            log.error(
                    "Failed to process payment.completed: " +
                            "topic={}, partition={}, offset={}, key={}, payload={}",
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    record.key(),
                    record.value(),
                    ex
            );

            /*
             * DO NOT commit the offset.
             *
             * The message can therefore be processed again.
             */
        }
    }


    // =========================================================
    // VALIDATE EVENT
    // =========================================================

    private void validateEvent(
            PaymentCompletedEvent event) {

        if (event == null) {

            throw new IllegalArgumentException(
                    "PaymentCompletedEvent must not be null"
            );
        }

        if (event.paymentId() == null) {

            throw new IllegalArgumentException(
                    "PaymentCompletedEvent paymentId is required"
            );
        }

        if (event.orderId() == null) {

            throw new IllegalArgumentException(
                    "PaymentCompletedEvent orderId is required"
            );
        }

        if (event.customerId() == null) {

            throw new IllegalArgumentException(
                    "PaymentCompletedEvent customerId is required"
            );
        }

        if (event.amount() == null) {

            throw new IllegalArgumentException(
                    "PaymentCompletedEvent amount is required"
            );
        }

        if (event.currency() == null ||
                event.currency().isBlank()) {

            throw new IllegalArgumentException(
                    "PaymentCompletedEvent currency is required"
            );
        }

        if (event.transactionId() == null ||
                event.transactionId().isBlank()) {

            throw new IllegalArgumentException(
                    "PaymentCompletedEvent transactionId is required"
            );
        }
    }


    // =========================================================
    // COMMIT OFFSET
    // =========================================================

    private void commitOffset(
            ConsumerRecord<String, String> record) {

        TopicPartition topicPartition =
                new TopicPartition(
                        record.topic(),
                        record.partition()
                );

        OffsetAndMetadata offset =
                new OffsetAndMetadata(
                        record.offset() + 1
                );

        kafkaConsumer.commitSync(
                Map.of(
                        topicPartition,
                        offset
                )
        );

        log.debug(
                "Kafka offset committed: " +
                        "topic={}, partition={}, offset={}",
                record.topic(),
                record.partition(),
                record.offset() + 1
        );
    }


    // =========================================================
    // STOP
    // =========================================================

    @PreDestroy
    public void stop() {

        log.info(
                "Stopping PaymentCompletedEventConsumer..."
        );

        running = false;


        if (kafkaConsumer != null) {

            /*
             * KafkaConsumer is not thread-safe.
             *
             * wakeup() is the correct way to interrupt
             * poll() from another thread.
             */
            kafkaConsumer.wakeup();
        }


        if (executorService != null) {

            executorService.shutdown();
        }
    }


    // =========================================================
    // CLOSE
    // =========================================================

    private void closeConsumer() {

        if (kafkaConsumer != null) {

            try {

                kafkaConsumer.close();

                log.info(
                        "PaymentCompletedEventConsumer closed"
                );

            } catch (Exception ex) {

                log.error(
                        "Error closing Kafka consumer",
                        ex
                );
            }
        }
    }
}