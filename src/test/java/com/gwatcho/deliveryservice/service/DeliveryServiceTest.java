package com.gwatcho.deliveryservice.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.gwatcho.deliveryservice.entity.Delivery;
import com.gwatcho.deliveryservice.entity.DeliveryStatus;
import com.gwatcho.deliveryservice.kafka.DeliveryCompletedEventProducer;
import com.gwatcho.deliveryservice.event.PaymentCompletedEvent;
import com.gwatcho.deliveryservice.repository.DeliveryRepository;
import java.math.BigDecimal;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DeliveryServiceTest {
    @Mock private DeliveryRepository deliveryRepository;

    @Mock
    private DeliveryCompletedEventProducer deliveryCompletedEventProducer;

    @InjectMocks private DeliveryService deliveryService;

    private PaymentCompletedEvent paymentCompletedEvent;

    @BeforeEach
    void setUp() {
        paymentCompletedEvent =
                new PaymentCompletedEvent(10L, 100L, 200L, new BigDecimal("2693.28"), "EUR", "TXN-123", null);
    }

    @Test
    void shouldStorePaymentInformationFromPaymentCompleted() {
        when(deliveryRepository.findByOrderId(100L)).thenReturn(Optional.empty());

        Delivery result = deliveryService.handlePaymentCompleted(paymentCompletedEvent);

        assertThat(result).isNotNull();
        assertThat(result.getId()).isNull();
        assertThat(result.getOrderId()).isEqualTo(100L);
        assertThat(result.getCustomerId()).isEqualTo(200L);
        assertThat(result.getPaymentId()).isEqualTo(10L);
        assertThat(result.getAmount()).isEqualByComparingTo(new BigDecimal("2693.28"));
        assertThat(result.getCurrency()).isEqualTo("EUR");
        assertThat(result.getTransactionId()).isEqualTo("TXN-123");
        assertThat(result.getStatus()).isEqualTo(DeliveryStatus.CREATED);

        verify(deliveryRepository, never()).save(any(Delivery.class));
    }

    @Test
    void shouldCreateDeliveryWhenPaymentArrivesBeforeOrder() {
        when(deliveryRepository.findByOrderId(100L)).thenReturn(Optional.empty());

        Delivery result = deliveryService.handlePaymentCompleted(paymentCompletedEvent);

        assertThat(result).isNotNull();
        assertThat(result.getId()).isNull();
        assertThat(result.getOrderId()).isEqualTo(100L);
        assertThat(result.getCustomerId()).isEqualTo(200L);
        assertThat(result.getPaymentId()).isEqualTo(10L);

        verify(deliveryRepository, never()).save(any(Delivery.class));
    }

    @Test
    void shouldCorrelateEventsUsingOrderId() {
        Delivery existingDelivery =
                Delivery.builder().id(1L).orderId(100L).customerId(200L).status(DeliveryStatus.CREATED).build();

        when(deliveryRepository.findByOrderId(100L)).thenReturn(Optional.of(existingDelivery));

        when(deliveryRepository.save(any(Delivery.class))).thenAnswer(invocation -> invocation.getArgument(0));

        Delivery result = deliveryService.handlePaymentCompleted(paymentCompletedEvent);

        assertThat(result).isSameAs(existingDelivery);
        assertThat(result.getId()).isEqualTo(1L);
        assertThat(result.getOrderId()).isEqualTo(100L);
        assertThat(result.getCustomerId()).isEqualTo(200L);
        assertThat(result.getPaymentId()).isEqualTo(10L);
        assertThat(result.getAmount()).isEqualByComparingTo(new BigDecimal("2693.28"));
        assertThat(result.getCurrency()).isEqualTo("EUR");
        assertThat(result.getTransactionId()).isEqualTo("TXN-123");
        assertThat(result.getStatus()).isEqualTo(DeliveryStatus.CREATED);

        verify(deliveryRepository).save(existingDelivery);
    }

    @Test
    void shouldCreateSeparateCorrelationForDifferentOrder() {
        Delivery existingDelivery =
                Delivery.builder().id(1L).orderId(200L).customerId(300L).status(DeliveryStatus.CREATED).build();

        when(deliveryRepository.findByOrderId(100L)).thenReturn(Optional.empty());

        Delivery result = deliveryService.handlePaymentCompleted(paymentCompletedEvent);

        assertThat(result).isNotNull();
        assertThat(result.getId()).isNull();
        assertThat(result.getOrderId()).isEqualTo(100L);
        assertThat(result.getCustomerId()).isEqualTo(200L);

        verify(deliveryRepository, never()).save(any(Delivery.class));
    }

    @Test
    void shouldNotCreateAnotherDeliveryForDuplicatePaymentEvent() {
        Delivery existingDelivery =
                Delivery.builder().id(1L).orderId(100L).customerId(200L).status(DeliveryStatus.CREATED).build();

        when(deliveryRepository.findByOrderId(100L)).thenReturn(Optional.of(existingDelivery));

        when(deliveryRepository.save(any(Delivery.class))).thenAnswer(invocation -> invocation.getArgument(0));

        Delivery firstResult = deliveryService.handlePaymentCompleted(paymentCompletedEvent);

        Delivery secondResult = deliveryService.handlePaymentCompleted(paymentCompletedEvent);

        assertThat(firstResult).isSameAs(existingDelivery);
        assertThat(secondResult).isSameAs(existingDelivery);

        assertThat(secondResult.getPaymentId()).isEqualTo(10L);
        assertThat(secondResult.getTransactionId()).isEqualTo("TXN-123");

        verify(deliveryRepository, times(2)).save(existingDelivery);
    }

    @Test
    void shouldChangeStatusFromCreatedToPreparing() {
        Delivery delivery = Delivery.builder().id(1L).orderId(100L).customerId(200L).status(DeliveryStatus.CREATED).build();

        when(deliveryRepository.findById(1L)).thenReturn(Optional.of(delivery));

        when(deliveryRepository.save(any(Delivery.class))).thenAnswer(invocation -> invocation.getArgument(0));

        Delivery result = deliveryService.updateStatus(1L, DeliveryStatus.PREPARING);

        assertThat(result.getStatus()).isEqualTo(DeliveryStatus.PREPARING);

        verify(deliveryRepository).save(delivery);
        verifyNoInteractions(deliveryCompletedEventProducer);
    }

    @Test
    void shouldChangeStatusFromPreparingToShipped() {
        Delivery delivery =
                Delivery.builder().id(1L).orderId(100L).customerId(200L).status(DeliveryStatus.PREPARING).build();

        when(deliveryRepository.findById(1L)).thenReturn(Optional.of(delivery));

        when(deliveryRepository.save(any(Delivery.class))).thenAnswer(invocation -> invocation.getArgument(0));

        Delivery result = deliveryService.updateStatus(1L, DeliveryStatus.SHIPPED);

        assertThat(result.getStatus()).isEqualTo(DeliveryStatus.SHIPPED);

        verify(deliveryRepository).save(delivery);
        verifyNoInteractions(deliveryCompletedEventProducer);
    }

    @Test
    void shouldChangeStatusFromShippedToDelivered() {
        Delivery delivery = Delivery.builder().id(1L).orderId(100L).customerId(200L).status(DeliveryStatus.SHIPPED).build();

        when(deliveryRepository.findById(1L)).thenReturn(Optional.of(delivery));

        when(deliveryRepository.save(any(Delivery.class))).thenAnswer(invocation -> invocation.getArgument(0));

        Delivery result = deliveryService.updateStatus(1L, DeliveryStatus.DELIVERED);

        assertThat(result.getStatus()).isEqualTo(DeliveryStatus.DELIVERED);

        assertThat(result.getDeliveredAt()).isNotNull();

        verify(deliveryRepository).save(delivery);

        verify(deliveryCompletedEventProducer).publish(any());
    }

    @Test
    void shouldCancelCreatedDelivery() {
        Delivery delivery = Delivery.builder().id(1L).orderId(100L).customerId(200L).status(DeliveryStatus.CREATED).build();

        when(deliveryRepository.findById(1L)).thenReturn(Optional.of(delivery));

        when(deliveryRepository.save(any(Delivery.class))).thenAnswer(invocation -> invocation.getArgument(0));

        Delivery result = deliveryService.updateStatus(1L, DeliveryStatus.CANCELLED);

        assertThat(result.getStatus()).isEqualTo(DeliveryStatus.CANCELLED);

        verify(deliveryRepository).save(delivery);
        verifyNoInteractions(deliveryCompletedEventProducer);
    }

    @Test
    void shouldRejectInvalidStatusTransition() {
        Delivery delivery = Delivery.builder().id(1L).orderId(100L).customerId(200L).status(DeliveryStatus.CREATED).build();

        when(deliveryRepository.findById(1L)).thenReturn(Optional.of(delivery));

        assertThatThrownBy(() -> deliveryService.updateStatus(1L, DeliveryStatus.SHIPPED))
                .isInstanceOf(IllegalStateException.class);

        verify(deliveryRepository, never()).save(any(Delivery.class));

        verifyNoInteractions(deliveryCompletedEventProducer);
    }

    @Test
    void shouldRejectUpdateForUnknownDelivery() {
        when(deliveryRepository.findById(999L)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> deliveryService.updateStatus(999L, DeliveryStatus.PREPARING))
                .isInstanceOf(IllegalArgumentException.class);

        verify(deliveryRepository, never()).save(any(Delivery.class));

        verifyNoInteractions(deliveryCompletedEventProducer);
    }

    @Test
    void shouldRejectGetForUnknownDelivery() {
        when(deliveryRepository.findById(999L)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> deliveryService.getDelivery(999L)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldRejectGetByOrderIdForUnknownOrder() {
        when(deliveryRepository.findByOrderId(999L)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> deliveryService.getByOrderId(999L)).isInstanceOf(IllegalArgumentException.class);
    }
}
