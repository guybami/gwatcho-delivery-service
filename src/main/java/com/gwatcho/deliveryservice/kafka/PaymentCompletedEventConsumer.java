package com.gwatcho.deliveryservice.kafka;

import com.gwatcho.deliveryservice.event.PaymentCompletedEvent;
import com.gwatcho.deliveryservice.service.DeliveryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class PaymentCompletedEventConsumer {
    private final DeliveryService deliveryService;

    @KafkaListener(topics = "${app.kafka.topics.payment-completed}", groupId = "${app.kafka.consumer.group-id}")
    public void consumePaymentCompleted(ConsumerRecord<String, PaymentCompletedEvent> record) {
        PaymentCompletedEvent event = record.value();

        log.info("Received payment.completed: topic={}, partition={}, offset={}, key={}", record.topic(),
                record.partition(), record.offset(), record.key());

        try {
            validateEvent(event);

            log.info("PaymentCompletedEvent received: "
                            + "paymentId={}, orderId={}, customerId={}, "
                            + "amount={}, currency={}, transactionId={}",
                    event.paymentId(), event.orderId(), event.customerId(), event.amount(), event.currency(),
                    event.transactionId());

            // -------------------------------------------------
            // Business logic
            // -------------------------------------------------

            deliveryService.handlePaymentCompleted(event);

            log.info("payment.completed processed successfully: "
                            + "orderId={}, partition={}, offset={}",
                    event.orderId(), record.partition(), record.offset());

        } catch (Exception ex) {
            log.error("Failed to process payment.completed: "
                            + "topic={}, partition={}, offset={}, key={}",
                    record.topic(), record.partition(), record.offset(), record.key(), ex);

            // Do not silently acknowledge a failed message.
            // Let Spring Kafka handle the failure.
            throw ex;
        }
    }

    private void validateEvent(PaymentCompletedEvent event) {
        if (event == null) {
            throw new IllegalArgumentException("PaymentCompletedEvent must not be null");
        }

        if (event.paymentId() == null) {
            throw new IllegalArgumentException("PaymentCompletedEvent paymentId is required");
        }

        if (event.orderId() == null) {
            throw new IllegalArgumentException("PaymentCompletedEvent orderId is required");
        }

        if (event.customerId() == null) {
            throw new IllegalArgumentException("PaymentCompletedEvent customerId is required");
        }

        if (event.amount() == null) {
            throw new IllegalArgumentException("PaymentCompletedEvent amount is required");
        }

        if (event.currency() == null || event.currency().isBlank()) {
            throw new IllegalArgumentException("PaymentCompletedEvent currency is required");
        }

        if (event.transactionId() == null || event.transactionId().isBlank()) {
            throw new IllegalArgumentException("PaymentCompletedEvent transactionId is required");
        }
    }
}