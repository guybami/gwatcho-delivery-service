package com.gwatcho.deliveryservice.integration;

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
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest
@ActiveProfiles("test")
class DeliveryKafkaIntegrationTest {
    private static final String PAYMENT_COMPLETED_TOPIC = "payment.completed";

    @Autowired private KafkaTemplate<String, PaymentCompletedEvent> kafkaTemplate;

    @Autowired private DeliveryRepository deliveryRepository;

    @Test
    void shouldCompleteExistingDeliveryFromPaymentCompletedEvent() throws Exception {
        Long orderId = System.currentTimeMillis();
        Long customerId = 200L;

        Long existingPaymentId = 5L;
        Long completedPaymentId = 10L;

        BigDecimal amount = new BigDecimal("2693.28");
        String currency = "EUR";
        String transactionId = "TXN-" + UUID.randomUUID();

        /*
         * Create an existing delivery.
         *
         * The real MySQL schema requires payment_id,
         * amount, currency and status to be non-null.
         */
        Delivery delivery = Delivery.builder()
                .orderId(orderId)
                .customerId(customerId)
                .paymentId(existingPaymentId)
                .amount(amount)
                .currency(currency)
                .street("Main Street 10")
                .postalCode("74172")
                .city("Neckarsulm")
                .country("DE")
                .status(DeliveryStatus.CREATED)
                .build();

        Delivery persistedDelivery = deliveryRepository.save(delivery);

        assertThat(persistedDelivery.getId()).isNotNull();

        /*
         * Create the payment.completed event.
         */
        PaymentCompletedEvent event = new PaymentCompletedEvent(completedPaymentId, orderId, customerId, amount, currency,
                transactionId, new DeliveryAddress("Main Street 10", "74172", "Neckarsulm", "DE"));

        /*
         * Publish the event to Kafka.
         */
        kafkaTemplate.send(PAYMENT_COMPLETED_TOPIC, orderId.toString(), event).get();

        /*
         * Kafka processing is asynchronous.
         *
         * Wait until the Delivery Service consumer has
         * processed the payment.completed event.
         */
        Delivery result = waitForPaymentUpdate(orderId, completedPaymentId, 15);

        /*
         * Verify that the existing delivery was updated.
         */
        assertThat(result.getId()).isEqualTo(persistedDelivery.getId());

        assertThat(result.getOrderId()).isEqualTo(orderId);

        assertThat(result.getCustomerId()).isEqualTo(customerId);

        assertThat(result.getPaymentId()).isEqualTo(completedPaymentId);

        assertThat(result.getAmount()).isEqualByComparingTo(amount);

        assertThat(result.getCurrency()).isEqualTo(currency);

        assertThat(result.getTransactionId()).isEqualTo(transactionId);

        assertThat(result.getStreet()).isEqualTo("Main Street 10");

        assertThat(result.getPostalCode()).isEqualTo("74172");

        assertThat(result.getCity()).isEqualTo("Neckarsulm");

        assertThat(result.getCountry()).isEqualTo("DE");

        assertThat(result.getStatus()).isEqualTo(DeliveryStatus.CREATED);
    }

    private Delivery waitForPaymentUpdate(Long orderId, Long expectedPaymentId, int timeoutSeconds) {
        long timeout = System.currentTimeMillis() + timeoutSeconds * 1000L;

        Delivery latestDelivery = null;

        while (System.currentTimeMillis() < timeout) {
            latestDelivery = deliveryRepository.findByOrderId(orderId).orElse(null);

            if (latestDelivery != null && expectedPaymentId.equals(latestDelivery.getPaymentId())
                    && latestDelivery.getStatus() == DeliveryStatus.CREATED) {
                return latestDelivery;
            }

            sleep(250);
        }

        throw new AssertionError("Timed out waiting for payment.completed processing. "
                + "orderId=" + orderId + ", expectedPaymentId=" + expectedPaymentId
                + ", actualPaymentId=" + (latestDelivery != null ? latestDelivery.getPaymentId() : null));
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