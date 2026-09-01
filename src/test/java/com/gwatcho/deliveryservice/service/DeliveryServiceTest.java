
package com.gwatcho.deliveryservice.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.gwatcho.deliveryservice.dto.DeliveryAddress;
import com.gwatcho.deliveryservice.entity.Delivery;
import com.gwatcho.deliveryservice.entity.DeliveryStatus;
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

    @InjectMocks private DeliveryService deliveryService;

    private PaymentCompletedEvent paymentCompletedEvent;

    // =========================================================
    // SETUP
    // =========================================================

    @BeforeEach
    void setUp() {
        lenient().when(deliveryRepository.save(any(Delivery.class))).thenAnswer(invocation -> {
            Delivery delivery = invocation.getArgument(0);

            if (delivery.getId() == null) {
                delivery.setId(1L);
            }

            return delivery;
        });

        paymentCompletedEvent = new PaymentCompletedEvent(10L, 100L, 200L, new BigDecimal("2693.28"), "EUR",
                "TXN-123456", new DeliveryAddress("Main Street 10", "74172", "Neckarsulm", "DE"));
    }


  // =========================================================
    // PAYMENT.COMPLETED
    // =========================================================

    @Test
    void shouldStorePaymentInformationFromPaymentCompleted() {
        when(deliveryRepository.findByOrderId(100L)).thenReturn(Optional.empty());

        Delivery result = deliveryService.handlePaymentCompleted(paymentCompletedEvent);

        assertThat(result).isNotNull();

        assertThat(result.getId()).isNull();

        assertThat(result.getOrderId()).isEqualTo(100L);

        assertThat(result.getPaymentId()).isEqualTo(10L);

        assertThat(result.getCustomerId()).isEqualTo(200L);

        assertThat(result.getAmount()).isEqualByComparingTo(new BigDecimal("2693.28"));

        assertThat(result.getCurrency()).isEqualTo("EUR");

        assertThat(result.getTransactionId()).isEqualTo("TXN-123456");

        /*
         * ORDER.CREATED has not arrived yet.
         *
         * The payment information is kept as a pending correlation,
         * but an incomplete Delivery must NOT be persisted.
         */
        assertThat(result.getStreet()).isNull();

        assertThat(result.getPostalCode()).isNull();

        assertThat(result.getCity()).isNull();

        assertThat(result.getCountry()).isNull();

        assertThat(result.getStatus()).isNull();

        verify(deliveryRepository, never()).save(any(Delivery.class));
    }



    // =========================================================
    // PAYMENT.COMPLETED -> ORDER.CREATED
    // =========================================================

    @Test
    void shouldCreateDeliveryWhenPaymentArrivesBeforeOrder() {
        when(deliveryRepository.findByOrderId(100L)).thenReturn(Optional.empty(), Optional.empty());

        /*
         * Payment arrives first.
         *
         * No incomplete Delivery is persisted.
         */
        Delivery afterPayment = deliveryService.handlePaymentCompleted(paymentCompletedEvent);

        assertThat(afterPayment).isNotNull();

        assertThat(afterPayment.getId()).isNull();

        assertThat(afterPayment.getOrderId()).isEqualTo(100L);

        assertThat(afterPayment.getPaymentId()).isEqualTo(10L);

        assertThat(afterPayment.getCustomerId()).isEqualTo(200L);

        assertThat(afterPayment.getAmount()).isEqualByComparingTo(new BigDecimal("2693.28"));

        assertThat(afterPayment.getCurrency()).isEqualTo("EUR");

        assertThat(afterPayment.getTransactionId()).isEqualTo("TXN-123456");

        assertThat(afterPayment.getStatus()).isNull();

        verify(deliveryRepository, never()).save(any(Delivery.class));


        /*
         * Only the complete Delivery is persisted.
         *
         * 1 save from tryCreateDelivery()
         */
        //verify(deliveryRepository, times(1)).save(any(Delivery.class));
    }

    // =========================================================
    // CORRELATION BY ORDER ID
    // =========================================================
    @Test
    void shouldCorrelateEventsUsingOrderId() {
        Delivery delivery = Delivery.builder()
                .id(1L)
                .orderId(100L)
                .customerId(200L)
                .street("Main Street 10")
                .postalCode("74172")
                .city("Neckarsulm")
                .country("DE")
                .build();

        when(deliveryRepository.findByOrderId(100L)).thenReturn(Optional.of(delivery));

        Delivery result = deliveryService.handlePaymentCompleted(paymentCompletedEvent);

        assertThat(result.getOrderId()).isEqualTo(100L);

        assertThat(result.getPaymentId()).isEqualTo(10L);

        assertThat(result.getStreet()).isEqualTo("Main Street 10");

        assertThat(result.getPostalCode()).isEqualTo("74172");

        assertThat(result.getCity()).isEqualTo("Neckarsulm");

        assertThat(result.getCountry()).isEqualTo("DE");

        assertThat(result.getStatus()).isEqualTo(DeliveryStatus.CREATED);

        verify(deliveryRepository).findByOrderId(100L);

        verify(deliveryRepository).save(delivery);
    }

    // =========================================================
    // DIFFERENT ORDER ID
    // =========================================================

    @Test
    void shouldCreateSeparateCorrelationForDifferentOrder() {
        PaymentCompletedEvent event =
                new PaymentCompletedEvent(20L, 999L, 300L,
                        new BigDecimal("100.00"), "EUR", "TXN-999",
                        new DeliveryAddress("Main Street 10", "74172", "Neckarsulm", "DE"));

        when(deliveryRepository.findByOrderId(999L)).thenReturn(Optional.empty());

        Delivery result = deliveryService.handlePaymentCompleted(event);

        assertThat(result).isNotNull();

        assertThat(result.getId()).isNull();

        assertThat(result.getOrderId()).isEqualTo(999L);

        assertThat(result.getPaymentId()).isEqualTo(20L);

        assertThat(result.getCustomerId()).isEqualTo(300L);

        assertThat(result.getAmount()).isEqualByComparingTo(new BigDecimal("100.00"));

        assertThat(result.getCurrency()).isEqualTo("EUR");

        assertThat(result.getTransactionId()).isEqualTo("TXN-999");

        assertThat(result.getStatus()).isNull();

        verify(deliveryRepository, never()).save(any(Delivery.class));
    }

    // =========================================================
    // DUPLICATE ORDER.CREATED
    // =========================================================

    @Test
    void shouldNotCreateAnotherDeliveryForDuplicatePaymentEvent() {

        Delivery delivery =
                Delivery.builder()
                        .id(1L)
                        .orderId(100L)
                        .customerId(200L)
                        .paymentId(10L)
                        .amount(new BigDecimal("2693.28"))
                        .currency("EUR")
                        .transactionId("TXN-123456")
                        .street("Main Street 10")
                        .postalCode("74172")
                        .city("Neckarsulm")
                        .country("DE")
                        .status(DeliveryStatus.CREATED)
                        .build();

        when(deliveryRepository.findByOrderId(100L))
                .thenReturn(Optional.of(delivery));

        Delivery result = deliveryService.handlePaymentCompleted(
                        paymentCompletedEvent
                );

        assertThat(result)
                .isSameAs(delivery);

        assertThat(result.getId())
                .isEqualTo(1L);

        assertThat(result.getOrderId())
                .isEqualTo(100L);

        assertThat(result.getPaymentId())
                .isEqualTo(10L);

        assertThat(result.getStatus())
                .isEqualTo(DeliveryStatus.CREATED);

        verify(deliveryRepository)
                .findByOrderId(100L);


    }

    // =========================================================
    // DUPLICATE PAYMENT.COMPLETED
    // =========================================================

    //@Test
    void shouldNotCreateAnotherDeliveryForDuplicatePaymentEvent_old() {
        Delivery delivery = Delivery.builder()
                .id(1L)
                .orderId(100L)
                .customerId(200L)
                .street("Main Street 10")
                .postalCode("74172")
                .city("Neckarsulm")
                .country("DE")
                .paymentId(10L)
                .amount(new BigDecimal("2693.28"))
                .currency("EUR")
                .transactionId("TXN-123456")
                .status(DeliveryStatus.CREATED)
                .build();

        when(deliveryRepository.findByOrderId(100L)).thenReturn(Optional.of(delivery));

        Delivery result = deliveryService.handlePaymentCompleted(paymentCompletedEvent);

        assertThat(result.getId()).isEqualTo(1L);

        assertThat(result.getOrderId()).isEqualTo(100L);

        assertThat(result.getPaymentId()).isEqualTo(10L);

        assertThat(result.getStatus()).isEqualTo(DeliveryStatus.CREATED);

        verify(deliveryRepository).save(delivery);
    }

    // =========================================================
    // GET BY ID
    // =========================================================

    @Test
    void shouldGetDeliveryById() {
        Delivery delivery = Delivery.builder().id(1L).orderId(100L).status(DeliveryStatus.CREATED).build();

        when(deliveryRepository.findById(1L)).thenReturn(Optional.of(delivery));

        Delivery result = deliveryService.getDelivery(1L);

        assertThat(result).isSameAs(delivery);

        verify(deliveryRepository).findById(1L);
    }

    // =========================================================
    // GET BY ORDER ID
    // =========================================================

    @Test
    void shouldGetDeliveryByOrderId() {
        Delivery delivery = Delivery.builder().id(1L).orderId(100L).status(DeliveryStatus.CREATED).build();

        when(deliveryRepository.findByOrderId(100L)).thenReturn(Optional.of(delivery));

        Delivery result = deliveryService.getByOrderId(100L);

        assertThat(result).isSameAs(delivery);

        verify(deliveryRepository).findByOrderId(100L);
    }

    // =========================================================
    // DELIVERY NOT FOUND
    // =========================================================

    @Test
    void shouldThrowWhenDeliveryNotFound() {
        when(deliveryRepository.findById(999L)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> deliveryService.getDelivery(999L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Delivery not found: 999");

        verify(deliveryRepository).findById(999L);

        verify(deliveryRepository, never()).save(any());
    }

    // =========================================================
    // DELIVERY NOT FOUND BY ORDER
    // =========================================================

    @Test
    void shouldThrowWhenDeliveryNotFoundForOrder() {
        when(deliveryRepository.findByOrderId(999L)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> deliveryService.getByOrderId(999L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Delivery not found for order: 999");

        verify(deliveryRepository).findByOrderId(999L);

        verify(deliveryRepository, never()).save(any());
    }

    // =========================================================
    // CREATED -> PREPARING
    // =========================================================

    @Test
    void shouldChangeStatusFromCreatedToPreparing() {
        Delivery delivery = Delivery.builder().id(1L).orderId(100L).status(DeliveryStatus.CREATED).build();

        when(deliveryRepository.findById(1L)).thenReturn(Optional.of(delivery));

        Delivery result = deliveryService.updateStatus(1L, DeliveryStatus.PREPARING);

        assertThat(result).isNotNull();

        assertThat(result.getStatus()).isEqualTo(DeliveryStatus.PREPARING);

        verify(deliveryRepository).save(delivery);
    }

    // =========================================================
    // PREPARING -> SHIPPED
    // =========================================================

    @Test
    void shouldChangeStatusFromPreparingToShipped() {
        Delivery delivery = Delivery.builder().id(1L).orderId(100L).status(DeliveryStatus.PREPARING).build();

        when(deliveryRepository.findById(1L)).thenReturn(Optional.of(delivery));

        Delivery result = deliveryService.updateStatus(1L, DeliveryStatus.SHIPPED);

        assertThat(result).isNotNull();

        assertThat(result.getStatus()).isEqualTo(DeliveryStatus.SHIPPED);

        verify(deliveryRepository).save(delivery);
    }

    // =========================================================
    // SHIPPED -> DELIVERED
    // =========================================================

    @Test
    void shouldChangeStatusFromShippedToDelivered() {
        Delivery delivery = Delivery.builder().id(1L).orderId(100L).status(DeliveryStatus.SHIPPED).build();

        when(deliveryRepository.findById(1L)).thenReturn(Optional.of(delivery));

        Delivery result = deliveryService.updateStatus(1L, DeliveryStatus.DELIVERED);

        assertThat(result).isNotNull();

        assertThat(result.getStatus()).isEqualTo(DeliveryStatus.DELIVERED);

        assertThat(result.getDeliveredAt()).isNotNull();

        verify(deliveryRepository).save(delivery);
    }

    // =========================================================
    // CREATED -> CANCELLED
    // =========================================================

    @Test
    void shouldCancelCreatedDelivery() {
        Delivery delivery = Delivery.builder().id(1L).orderId(100L).status(DeliveryStatus.CREATED).build();

        when(deliveryRepository.findById(1L)).thenReturn(Optional.of(delivery));

        Delivery result = deliveryService.updateStatus(1L, DeliveryStatus.CANCELLED);

        assertThat(result).isNotNull();

        assertThat(result.getStatus()).isEqualTo(DeliveryStatus.CANCELLED);

        verify(deliveryRepository).save(delivery);
    }

    // =========================================================
    // INVALID TRANSITION
    // =========================================================

    @Test
    void shouldRejectInvalidStatusTransition() {
        Delivery delivery = Delivery.builder().id(1L).orderId(100L).status(DeliveryStatus.CREATED).build();

        when(deliveryRepository.findById(1L)).thenReturn(Optional.of(delivery));

        assertThatThrownBy(() -> deliveryService.updateStatus(1L, DeliveryStatus.DELIVERED))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Invalid delivery status transition");

        verify(deliveryRepository, never()).save(any());
    }
}