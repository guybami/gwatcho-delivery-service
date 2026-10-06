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
import com.gwatcho.deliveryservice.event.DeliveryCompletedEvent;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class DeliveryCompletedEventProducer {

    private final KafkaTemplate<String, DeliveryCompletedEvent> kafkaTemplate;

    public DeliveryCompletedEventProducer(
            KafkaTemplate<String, DeliveryCompletedEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publish(DeliveryCompletedEvent event) {
        try {
            kafkaTemplate.send(
                    "delivery.completed",
                    event.orderId().toString(),
                    event
            );
        } catch (Exception e) {
            throw new IllegalStateException("Failed to serialize delivery.completed event", e);
        }
    }
}