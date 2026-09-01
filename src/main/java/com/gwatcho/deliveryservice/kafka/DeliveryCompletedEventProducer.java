package com.gwatcho.deliveryservice.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gwatcho.deliveryservice.event.DeliveryCompletedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class DeliveryCompletedEventProducer {
    private final KafkaProducer<String, String> kafkaProducer;
    private final ObjectMapper objectMapper;
    private final String topic;

    public DeliveryCompletedEventProducer(KafkaProducer<String, String> kafkaProducer, ObjectMapper objectMapper,
                                          @Value("${app.kafka.topics.delivery-completed}") String topic) {
        this.kafkaProducer = kafkaProducer;
        this.objectMapper = objectMapper;
        this.topic = topic;
    }

    public void publish(DeliveryCompletedEvent event) {
        try {
            String payload = objectMapper.writeValueAsString(event);

            ProducerRecord<String, String> record = new ProducerRecord<>(topic, String.valueOf(event.orderId()), payload);

            kafkaProducer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    throw new IllegalStateException("Failed to publish delivery.completed", exception);
                }
            });

        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize delivery.completed event", e);
        }
    }
}