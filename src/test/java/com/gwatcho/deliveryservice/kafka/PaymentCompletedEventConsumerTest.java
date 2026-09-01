
        package com.gwatcho.deliveryservice.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gwatcho.deliveryservice.dto.DeliveryAddress;
import com.gwatcho.deliveryservice.event.PaymentCompletedEvent;
import com.gwatcho.deliveryservice.service.DeliveryService;
import java.math.BigDecimal;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PaymentCompletedEventConsumerTest {
    @Mock private DeliveryService deliveryService;

    @Mock private KafkaConsumer<String, String> kafkaConsumer;

    private ObjectMapper objectMapper;

    private PaymentCompletedEventConsumer consumer;

    private PaymentCompletedEvent event;

    private String payload;

    // =========================================================
    // SETUP
    // =========================================================

    @BeforeEach
    void setUp() throws Exception {
        objectMapper = new ObjectMapper().findAndRegisterModules();

        consumer = new PaymentCompletedEventConsumer(deliveryService, objectMapper);

        /*
         * PaymentCompletedEventConsumer creates the native
         * KafkaConsumer itself during start().
         *
         * For this unit test we inject a mocked KafkaConsumer
         * so that processRecord() can be tested without Kafka.
         */
        var field = PaymentCompletedEventConsumer.class.getDeclaredField("kafkaConsumer");

        field.setAccessible(true);

        field.set(consumer, kafkaConsumer);

        event = new PaymentCompletedEvent(10L, 100L, 200L,
                new BigDecimal("2693.28"), "EUR", "TXN-123456",
                new DeliveryAddress("Main Street 10", "74172", "Neckarsulm", "DE"));

        payload = objectMapper.writeValueAsString(event);
    }

    // =========================================================
    // VALID EVENT
    // =========================================================

    @Test
    void shouldProcessValidPaymentCompletedEvent() {
        ConsumerRecord<String, String> record = new ConsumerRecord<>("payment.completed", 0, 10L, "100", payload);

        consumer.processRecord(record);

        verify(deliveryService).handlePaymentCompleted(any(PaymentCompletedEvent.class));

        verify(kafkaConsumer).commitSync(any(Map.class));
    }

    // =========================================================
    // DESERIALIZATION
    // =========================================================

    @Test
    void shouldDeserializePaymentCompletedEventCorrectly() {
        ConsumerRecord<String, String> record = new ConsumerRecord<>("payment.completed", 0, 10L, "100", payload);

        consumer.processRecord(record);

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
    // OFFSET COMMIT
    // =========================================================

    @Test
    void shouldCommitOffsetAfterSuccessfulProcessing() {
        ConsumerRecord<String, String> record = new ConsumerRecord<>("payment.completed", 2, 50L, "100", payload);

        consumer.processRecord(record);

        verify(deliveryService).handlePaymentCompleted(any(PaymentCompletedEvent.class));

        verify(kafkaConsumer).commitSync(any(Map.class));
    }

    // =========================================================
    // NO COMMIT WHEN SERVICE FAILS
    // =========================================================

    @Test
    void shouldNotCommitOffsetWhenDeliveryServiceFails() {
        doThrow(new IllegalStateException("DeliveryService failure"))
                .when(deliveryService)
                .handlePaymentCompleted(any(PaymentCompletedEvent.class));

        ConsumerRecord<String, String> record = new ConsumerRecord<>("payment.completed", 0, 10L, "100", payload);

        consumer.processRecord(record);

        verify(deliveryService).handlePaymentCompleted(any(PaymentCompletedEvent.class));

        verify(kafkaConsumer, never()).commitSync(any(Map.class));
    }

    // =========================================================
    // INVALID JSON
    // =========================================================

    @Test
    void shouldNotProcessInvalidJson() {
        ConsumerRecord<String, String> record =
                new ConsumerRecord<>("payment.completed", 0, 10L, "100", "{ invalid json }");

        consumer.processRecord(record);

        verify(deliveryService, never()).handlePaymentCompleted(any(PaymentCompletedEvent.class));

        verify(kafkaConsumer, never()).commitSync(any(Map.class));
    }

    // =========================================================
    // NULL PAYMENT ID
    // =========================================================

    @Test
    void shouldRejectEventWithoutPaymentId() throws Exception {
        PaymentCompletedEvent invalidEvent =
                new PaymentCompletedEvent(null, 100L, 200L, new BigDecimal("2693.28"), "EUR",
                        "TXN-123456", new DeliveryAddress("Main Street 10", "74172", "Neckarsulm", "DE"));

        String invalidPayload = objectMapper.writeValueAsString(invalidEvent);

        ConsumerRecord<String, String> record = new ConsumerRecord<>("payment.completed", 0, 10L, "100", invalidPayload);

        consumer.processRecord(record);

        verify(deliveryService, never()).handlePaymentCompleted(any(PaymentCompletedEvent.class));

        verify(kafkaConsumer, never()).commitSync(any(Map.class));
    }

    // =========================================================
    // NULL ORDER ID
    // =========================================================

    @Test
    void shouldRejectEventWithoutOrderId() throws Exception {
        PaymentCompletedEvent invalidEvent =
                new PaymentCompletedEvent(10L, null, 200L,
                        new BigDecimal("2693.28"), "EUR", "TXN-123456",
                        new DeliveryAddress("Main Street 10", "74172", "Neckarsulm", "DE"));

        String invalidPayload = objectMapper.writeValueAsString(invalidEvent);

        ConsumerRecord<String, String> record = new ConsumerRecord<>("payment.completed", 0, 11L, "null", invalidPayload);

        consumer.processRecord(record);

        verify(deliveryService, never()).handlePaymentCompleted(any(PaymentCompletedEvent.class));

        verify(kafkaConsumer, never()).commitSync(any(Map.class));
    }

    // =========================================================
    // NULL CUSTOMER ID
    // =========================================================

    @Test
    void shouldRejectEventWithoutCustomerId() throws Exception {
        PaymentCompletedEvent invalidEvent =
                new PaymentCompletedEvent(10L, 100L, null,
                        new BigDecimal("2693.28"), "EUR", "TXN-123456",
                        new DeliveryAddress("Main Street 10", "74172", "Neckarsulm", "DE"));

        String invalidPayload = objectMapper.writeValueAsString(invalidEvent);

        ConsumerRecord<String, String> record = new ConsumerRecord<>("payment.completed", 0, 12L, "100", invalidPayload);

        consumer.processRecord(record);

        verify(deliveryService, never()).handlePaymentCompleted(any(PaymentCompletedEvent.class));

        verify(kafkaConsumer, never()).commitSync(any(Map.class));
    }

    // =========================================================
    // NULL AMOUNT
    // =========================================================

    @Test
    void shouldRejectEventWithoutAmount() throws Exception {
        PaymentCompletedEvent invalidEvent = new PaymentCompletedEvent(10L, 100L,
                200L, null, "EUR", "TXN-123456",
                new DeliveryAddress("Main Street 10", "74172", "Neckarsulm", "DE"));

        String invalidPayload = objectMapper.writeValueAsString(invalidEvent);

        ConsumerRecord<String, String> record = new ConsumerRecord<>("payment.completed", 0, 13L, "100", invalidPayload);

        consumer.processRecord(record);

        verify(deliveryService, never()).handlePaymentCompleted(any(PaymentCompletedEvent.class));

        verify(kafkaConsumer, never()).commitSync(any(Map.class));
    }

    // =========================================================
    // EMPTY CURRENCY
    // =========================================================

    @Test
    void shouldRejectEventWithoutCurrency() throws Exception {
        PaymentCompletedEvent invalidEvent =
                new PaymentCompletedEvent(10L, 100L, 200L,
                        new BigDecimal("2693.28"), "", "TXN-123456",
                        new DeliveryAddress("Main Street 10", "74172", "Neckarsulm", "DE"));

        String invalidPayload = objectMapper.writeValueAsString(invalidEvent);

        ConsumerRecord<String, String> record = new ConsumerRecord<>("payment.completed", 0, 14L, "100", invalidPayload);

        consumer.processRecord(record);

        verify(deliveryService, never()).handlePaymentCompleted(any(PaymentCompletedEvent.class));

        verify(kafkaConsumer, never()).commitSync(any(Map.class));
    }

    // =========================================================
    // NULL TRANSACTION ID
    // =========================================================

    @Test
    void shouldRejectEventWithoutTransactionId() throws Exception {
        PaymentCompletedEvent invalidEvent =
                new PaymentCompletedEvent(10L, 100L, 200L,
                        new BigDecimal("2693.28"), "EUR", null,
                        new DeliveryAddress("Main Street 10", "74172", "Neckarsulm", "DE"));

        String invalidPayload = objectMapper.writeValueAsString(invalidEvent);

        ConsumerRecord<String, String> record = new ConsumerRecord<>("payment.completed", 0, 15L, "100", invalidPayload);

        consumer.processRecord(record);

        verify(deliveryService, never()).handlePaymentCompleted(any(PaymentCompletedEvent.class));

        verify(kafkaConsumer, never()).commitSync(any(Map.class));
    }

    // =========================================================
    // CORRECT OFFSET
    // =========================================================

    @Test
    void shouldCommitNextOffset() {
        ConsumerRecord<String, String> record = new ConsumerRecord<>("payment.completed", 3, 99L, "100", payload);

        consumer.processRecord(record);

        ArgumentCaptor<java.util.Map<org.apache.kafka.common.TopicPartition,
                org.apache.kafka.clients.consumer.OffsetAndMetadata>> captor = ArgumentCaptor.forClass(java.util.Map.class);

        verify(kafkaConsumer).commitSync(captor.capture());

        var committed = captor.getValue();

        var partition = new org.apache.kafka.common.TopicPartition("payment.completed", 3);

        assertThat(committed.get(partition)).isNotNull();

        assertThat(committed.get(partition).offset()).isEqualTo(100L);
    }
}

