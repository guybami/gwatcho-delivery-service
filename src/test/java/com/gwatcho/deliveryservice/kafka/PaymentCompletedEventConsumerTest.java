
package com.gwatcho.deliveryservice.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.fasterxml.jackson.databind.ObjectMapper;
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
    void shouldProcessValidPaymentCompletedEvent() throws Exception {
        ConsumerRecord<String, PaymentCompletedEvent> record = createRecord(event);

        consumer.consumePaymentCompleted(record);

        verify(deliveryService).handlePaymentCompleted(any(PaymentCompletedEvent.class));
    }

    // =========================================================
    // EVENT DATA
    // =========================================================

    @Test
    void shouldPassCorrectEventToDeliveryService()  {
        ConsumerRecord<String, PaymentCompletedEvent> record = createRecord(event);

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

        assertThat(result.deliveryAddress()).isNotNull();
        assertThat(result.deliveryAddress().street()).isEqualTo("Main Street 10");
        assertThat(result.deliveryAddress().postalCode()).isEqualTo("74172");
        assertThat(result.deliveryAddress().city()).isEqualTo("Neckarsulm");
        assertThat(result.deliveryAddress().country()).isEqualTo("DE");
    }

    // =========================================================
    // DELIVERY SERVICE FAILURE
    // =========================================================

    @Test
    void shouldPropagateDeliveryServiceFailure() throws Exception {
        doThrow(new IllegalStateException("DeliveryService failure"))
                .when(deliveryService)
                .handlePaymentCompleted(any(PaymentCompletedEvent.class));

        ConsumerRecord<String, PaymentCompletedEvent> record = createRecord(event);

        assertThrows(IllegalStateException.class, () -> consumer.consumePaymentCompleted(record));

        verify(deliveryService).handlePaymentCompleted(any(PaymentCompletedEvent.class));
    }

    // =========================================================
    // NULL PAYMENT ID
    // =========================================================
    /*
    //@Test
    void shouldRejectEventWithoutPaymentId() throws Exception {
        PaymentCompletedEvent invalidEvent = new PaymentCompletedEvent(
                null, 100L, 200L, new BigDecimal("2693.28"),
                "EUR", "TXN-123456", event.deliveryAddress());

        assertInvalidEvent(invalidEvent);
    }*/

    // =========================================================
    // NULL ORDER ID
    // =========================================================

    @Test
    void shouldRejectEventWithoutOrderId() throws Exception {
        PaymentCompletedEvent invalidEvent = new PaymentCompletedEvent(
                10L, null, 200L, new BigDecimal("2693.28"),
                "EUR", "TXN-123456", event.deliveryAddress());

        assertInvalidEvent(invalidEvent);
    }

    // =========================================================
    // NULL CUSTOMER ID
    // =========================================================

    @Test
    void shouldRejectEventWithoutCustomerId() throws Exception {
        PaymentCompletedEvent invalidEvent = new PaymentCompletedEvent(
                10L, 100L, null, new BigDecimal("2693.28"),
                "EUR", "TXN-123456", event.deliveryAddress());

        assertInvalidEvent(invalidEvent);
    }

    // =========================================================
    // NULL AMOUNT
    // =========================================================

    @Test
    void shouldRejectEventWithoutAmount() throws Exception {
        PaymentCompletedEvent invalidEvent =
                new PaymentCompletedEvent(10L, 100L, 200L,
                        null, "EUR", "TXN-123456", event.deliveryAddress());

        assertInvalidEvent(invalidEvent);
    }

    // =========================================================
    // EMPTY CURRENCY
    // =========================================================

    @Test
    void shouldRejectEventWithoutCurrency() throws Exception {
        PaymentCompletedEvent invalidEvent = new PaymentCompletedEvent(
                10L, 100L, 200L, new BigDecimal("2693.28"),
                "", "TXN-123456", event.deliveryAddress());

        assertInvalidEvent(invalidEvent);
    }

    // =========================================================
    // NULL TRANSACTION ID
    // =========================================================

    @Test
    void shouldRejectEventWithoutTransactionId() throws Exception {
        PaymentCompletedEvent invalidEvent =
                new PaymentCompletedEvent(10L, 100L, 200L, new BigDecimal("2693.28"), "EUR", null, event.deliveryAddress());

        assertInvalidEvent(invalidEvent);
    }

    // =========================================================
    // NULL EVENT
    // =========================================================

    @Test
    void shouldRejectNullEvent() {
        ConsumerRecord<String, PaymentCompletedEvent> record = new ConsumerRecord<>("payment.completed", 0, 20L, "100", null);

        assertThrows(IllegalArgumentException.class, () -> consumer.consumePaymentCompleted(record));

        verify(deliveryService, never()).handlePaymentCompleted(any(PaymentCompletedEvent.class));
    }


    // =========================================================
    // TEST HELPERS
    // =========================================================
    private ConsumerRecord<String, PaymentCompletedEvent> createRecord(PaymentCompletedEvent paymentEvent)   {
        return new ConsumerRecord<>("payment.completed", 0, 10L, "100", paymentEvent);
    }

    private void assertInvalidEvent(PaymentCompletedEvent invalidEvent) throws Exception {
        ConsumerRecord<String, PaymentCompletedEvent> record = createRecord(invalidEvent);

        assertThrows(IllegalArgumentException.class, () -> consumer.consumePaymentCompleted(record));

        verify(deliveryService, never()).handlePaymentCompleted(any(PaymentCompletedEvent.class));
    }


}