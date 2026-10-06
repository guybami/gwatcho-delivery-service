package com.gwatcho.deliveryservice.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.gwatcho.deliveryservice.dto.DeliveryAddress;
import com.gwatcho.deliveryservice.event.PaymentCompletedEvent;
import com.gwatcho.deliveryservice.service.DeliveryService;
import java.math.BigDecimal;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PaymentCompletedEventConsumerTest {
    @Mock private DeliveryService deliveryService;

    private PaymentCompletedEventConsumer consumer;

    private PaymentCompletedEvent event;

    @BeforeEach
    void setUp() {
        consumer = new PaymentCompletedEventConsumer(deliveryService);

        event = new PaymentCompletedEvent(10L, 100L, 200L, new BigDecimal("2693.28"), "EUR", "TXN-123456",
                new DeliveryAddress("Main Street 10", "74172", "Neckarsulm", "DE"));
    }

    // =========================================================
    // VALID EVENT
    // =========================================================

    @Test
    void shouldProcessValidPaymentCompletedEvent() {
        ConsumerRecord<String, PaymentCompletedEvent> record =
                new ConsumerRecord<>("payment.completed", 0, 10L, "100", event);

        consumer.consumePaymentCompleted(record);

        verify(deliveryService).handlePaymentCompleted(any(PaymentCompletedEvent.class));
    }

    // =========================================================
    // EVENT DATA
    // =========================================================

    @Test
    void shouldPassCorrectEventToDeliveryService() {
        ConsumerRecord<String, PaymentCompletedEvent> record =
                new ConsumerRecord<>("payment.completed", 0, 10L, "100", event);

        consumer.consumePaymentCompleted(record);

        ArgumentCaptor<PaymentCompletedEvent> captor = ArgumentCaptor.forClass(PaymentCompletedEvent.class);

        verify(deliveryService).handlePaymentCompleted(captor.capture());

        PaymentCompletedEvent result = captor.getValue();

        assertThat(result.paymentId()).isEqualTo(10L);
        assertThat(result.orderId()).isEqualTo(100L);
        assertThat(result.customerId()).isEqualTo(200L);
        assertThat(result.amount()).isEqualByComparingTo(new BigDecimal("2693.28"));
        assertThat(result.currency()).isEqualTo("EUR");
        assertThat(result.transactionId()).isEqualTo("TXN-123456");
    }

    // =========================================================
    // DELIVERY SERVICE FAILURE
    // =========================================================

    @Test
    void shouldPropagateDeliveryServiceFailure() {
        doThrow(new IllegalStateException("DeliveryService failure"))
                .when(deliveryService)
                .handlePaymentCompleted(any(PaymentCompletedEvent.class));

        ConsumerRecord<String, PaymentCompletedEvent> record =
                new ConsumerRecord<>("payment.completed", 0, 10L, "100", event);

        org.junit.jupiter.api.Assertions.assertThrows(
                IllegalStateException.class, () -> consumer.consumePaymentCompleted(record));

        verify(deliveryService).handlePaymentCompleted(any(PaymentCompletedEvent.class));
    }

    // =========================================================
    // NULL PAYMENT ID
    // =========================================================

    @Test
    void shouldRejectEventWithoutPaymentId() {
        PaymentCompletedEvent invalidEvent = new PaymentCompletedEvent(null, 100L, 200L, new BigDecimal("2693.28"), "EUR",
                "TXN-123456", new DeliveryAddress("Main Street 10", "74172", "Neckarsulm", "DE"));

        ConsumerRecord<String, PaymentCompletedEvent> record =
                new ConsumerRecord<>("payment.completed", 0, 10L, "100", invalidEvent);

        org.junit.jupiter.api.Assertions.assertThrows(
                IllegalArgumentException.class, () -> consumer.consumePaymentCompleted(record));

        verify(deliveryService, never()).handlePaymentCompleted(any(PaymentCompletedEvent.class));
    }

    // =========================================================
    // NULL ORDER ID
    // =========================================================

    @Test
    void shouldRejectEventWithoutOrderId() {
        PaymentCompletedEvent invalidEvent = new PaymentCompletedEvent(10L, null, 200L, new BigDecimal("2693.28"), "EUR",
                "TXN-123456", new DeliveryAddress("Main Street 10", "74172", "Neckarsulm", "DE"));

        ConsumerRecord<String, PaymentCompletedEvent> record =
                new ConsumerRecord<>("payment.completed", 0, 11L, "100", invalidEvent);

        org.junit.jupiter.api.Assertions.assertThrows(
                IllegalArgumentException.class, () -> consumer.consumePaymentCompleted(record));

        verify(deliveryService, never()).handlePaymentCompleted(any(PaymentCompletedEvent.class));
    }

    // =========================================================
    // NULL CUSTOMER ID
    // =========================================================

    @Test
    void shouldRejectEventWithoutCustomerId() {
        PaymentCompletedEvent invalidEvent = new PaymentCompletedEvent(10L, 100L, null, new BigDecimal("2693.28"), "EUR",
                "TXN-123456", new DeliveryAddress("Main Street 10", "74172", "Neckarsulm", "DE"));

        ConsumerRecord<String, PaymentCompletedEvent> record =
                new ConsumerRecord<>("payment.completed", 0, 12L, "100", invalidEvent);

        org.junit.jupiter.api.Assertions.assertThrows(
                IllegalArgumentException.class, () -> consumer.consumePaymentCompleted(record));

        verify(deliveryService, never()).handlePaymentCompleted(any(PaymentCompletedEvent.class));
    }

    // =========================================================
    // NULL AMOUNT
    // =========================================================

    @Test
    void shouldRejectEventWithoutAmount() {
        PaymentCompletedEvent invalidEvent = new PaymentCompletedEvent(
                10L, 100L, 200L, null, "EUR", "TXN-123456", new DeliveryAddress("Main Street 10", "74172", "Neckarsulm", "DE"));

        ConsumerRecord<String, PaymentCompletedEvent> record =
                new ConsumerRecord<>("payment.completed", 0, 13L, "100", invalidEvent);

        org.junit.jupiter.api.Assertions.assertThrows(
                IllegalArgumentException.class, () -> consumer.consumePaymentCompleted(record));

        verify(deliveryService, never()).handlePaymentCompleted(any(PaymentCompletedEvent.class));
    }

    // =========================================================
    // EMPTY CURRENCY
    // =========================================================

    @Test
    void shouldRejectEventWithoutCurrency() {
        PaymentCompletedEvent invalidEvent = new PaymentCompletedEvent(10L, 100L, 200L, new BigDecimal("2693.28"), "",
                "TXN-123456", new DeliveryAddress("Main Street 10", "74172", "Neckarsulm", "DE"));

        ConsumerRecord<String, PaymentCompletedEvent> record =
                new ConsumerRecord<>("payment.completed", 0, 14L, "100", invalidEvent);

        org.junit.jupiter.api.Assertions.assertThrows(
                IllegalArgumentException.class, () -> consumer.consumePaymentCompleted(record));

        verify(deliveryService, never()).handlePaymentCompleted(any(PaymentCompletedEvent.class));
    }

    // =========================================================
    // NULL TRANSACTION ID
    // =========================================================

    @Test
    void shouldRejectEventWithoutTransactionId() {
        PaymentCompletedEvent invalidEvent = new PaymentCompletedEvent(10L, 100L, 200L, new BigDecimal("2693.28"), "EUR",
                null, new DeliveryAddress("Main Street 10", "74172", "Neckarsulm", "DE"));

        ConsumerRecord<String, PaymentCompletedEvent> record =
                new ConsumerRecord<>("payment.completed", 0, 15L, "100", invalidEvent);

        org.junit.jupiter.api.Assertions.assertThrows(
                IllegalArgumentException.class, () -> consumer.consumePaymentCompleted(record));

        verify(deliveryService, never()).handlePaymentCompleted(any(PaymentCompletedEvent.class));
    }

    // =========================================================
    // NULL EVENT
    // =========================================================

    @Test
    void shouldRejectNullEvent() {
        ConsumerRecord<String, PaymentCompletedEvent> record =
                new ConsumerRecord<>("payment.completed", 0, 20L, "100", null);

        org.junit.jupiter.api.Assertions.assertThrows(
                IllegalArgumentException.class, () -> consumer.consumePaymentCompleted(record));

        verify(deliveryService, never()).handlePaymentCompleted(any(PaymentCompletedEvent.class));
    }
}