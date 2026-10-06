package com.gwatcho.deliveryservice.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import com.gwatcho.deliveryservice.dto.DeliveryAddress;
import com.gwatcho.deliveryservice.entity.Delivery;
import com.gwatcho.deliveryservice.entity.DeliveryStatus;
import com.gwatcho.deliveryservice.event.PaymentCompletedEvent;
import com.gwatcho.deliveryservice.repository.DeliveryRepository;
import java.math.BigDecimal;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;

@SpringBootTest
class DeliveryKafkaIntegrationTest {
    private static final String PAYMENT_COMPLETED_TOPIC = "payment.completed";
    @Autowired
    private KafkaTemplate<String, PaymentCompletedEvent> kafkaTemplate;
    @Autowired
    private DeliveryRepository deliveryRepository;

    @Test
    void shouldCreateDeliveryFromPaymentCompletedEvent() throws Exception {
        // ---------------------------------------------------------
        // 1. Test data
        // ---------------------------------------------------------

        Long orderId = System.currentTimeMillis();
        Long customerId = 200L;
        Long paymentId = 10L;

        String transactionId = "TXN-" + UUID.randomUUID();
        // ---------------------------------------------------------
        // 2. Build PaymentCompletedEvent
        // ---------------------------------------------------------

        PaymentCompletedEvent event = new PaymentCompletedEvent(paymentId, orderId, customerId, new BigDecimal("2693.28"),
                "EUR", transactionId, new DeliveryAddress("Main Street 10", "74172", "Neckarsulm", "DE"));

        // ---------------------------------------------------------
        // 3. Publish payment.completed
        // ---------------------------------------------------------
        kafkaTemplate.send(PAYMENT_COMPLETED_TOPIC, orderId.toString(), event).get();

        // ---------------------------------------------------------
        // 4. Wait for delivery-service consumer
        // ---------------------------------------------------------
        Delivery delivery = waitForDelivery(orderId, 15);

        // ---------------------------------------------------------
        // 5. Verify delivery
        // ---------------------------------------------------------
        assertThat(delivery).isNotNull();

        assertThat(delivery.getOrderId()).isEqualTo(orderId);

        assertThat(delivery.getCustomerId()).isEqualTo(customerId);

        assertThat(delivery.getPaymentId()).isEqualTo(paymentId);

        assertThat(delivery.getAmount()).isEqualByComparingTo(new BigDecimal("2693.28"));

        assertThat(delivery.getCurrency()).isEqualTo("EUR");

        assertThat(delivery.getTransactionId()).isEqualTo(transactionId);

        assertThat(delivery.getStreet()).isEqualTo("Main Street 10");

        assertThat(delivery.getPostalCode()).isEqualTo("74172");

        assertThat(delivery.getCity()).isEqualTo("Neckarsulm");

        assertThat(delivery.getCountry()).isEqualTo("DE");

        assertThat(delivery.getStatus()).isEqualTo(DeliveryStatus.CREATED);
    }

    // ---------------------------------------------------------
    // Wait for Kafka consumer
    // ---------------------------------------------------------

    private Delivery waitForDelivery(Long orderId, int timeoutSeconds) {
        long timeout = System.currentTimeMillis() + timeoutSeconds * 1000L;

        while (System.currentTimeMillis() < timeout) {
            Delivery delivery = deliveryRepository.findByOrderId(orderId).orElse(null);
            if (delivery != null) {
                return delivery;
            }
            sleep(250);
        }
        return deliveryRepository.findByOrderId(orderId).orElse(null);
    }

    private void sleep(long milliseconds) {
        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for Kafka event", e);
        }
    }
}