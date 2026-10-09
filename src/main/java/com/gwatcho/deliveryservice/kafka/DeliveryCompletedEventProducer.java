package com.gwatcho.deliveryservice.kafka;

import com.gwatcho.deliveryservice.event.DeliveryCompletedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class DeliveryCompletedEventProducer {

    private static final String TOPIC = "delivery.completed";

    private final KafkaTemplate<String, DeliveryCompletedEvent> kafkaTemplate;

    public void publish(DeliveryCompletedEvent event) {

        String key = event.orderId().toString();

        kafkaTemplate.send(TOPIC, key, event);

        log.info(
                "Delivery completed event published: deliveryId={}, orderId={}, customerId={}",
                event.deliveryId(),
                event.orderId(),
                event.customerId());
    }
}
