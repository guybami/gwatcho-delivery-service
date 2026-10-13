
package com.gwatcho.deliveryservice.kafka;

import com.gwatcho.deliveryservice.event.OrderCreatedEvent;
import com.gwatcho.deliveryservice.service.DeliveryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class OrderCreatedEventConsumer {
    private final DeliveryService deliveryService;

    @KafkaListener(topics = "${app.kafka.topics.order-created}", groupId = "${app.kafka.consumer.group-id}",
            containerFactory = "orderCreatedKafkaListenerContainerFactory")
    public void
    consume(ConsumerRecord<String, OrderCreatedEvent> record) {
        OrderCreatedEvent event = record.value();
        if (event == null) {
            throw new IllegalArgumentException("OrderCreatedEvent must not be null");
        }

        if (event.orderId() == null || event.customerId() == null) {
            throw new IllegalArgumentException("OrderCreatedEvent requires orderId and customerId");
        }

        log.info("Received order.created: orderId={}, partition={}, offset={}", event.orderId(),
                record.partition(), record.offset());

        deliveryService.handleOrderCreated(event);
    }
}