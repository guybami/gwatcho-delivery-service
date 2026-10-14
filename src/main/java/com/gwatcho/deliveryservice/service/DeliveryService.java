
package com.gwatcho.deliveryservice.service;

import com.gwatcho.deliveryservice.entity.Delivery;
import com.gwatcho.deliveryservice.entity.DeliveryStatus;
import com.gwatcho.deliveryservice.event.DeliveryCompletedEvent;
import com.gwatcho.deliveryservice.event.OrderCreatedEvent;
import com.gwatcho.deliveryservice.event.PaymentCompletedEvent;
import com.gwatcho.deliveryservice.kafka.DeliveryCompletedEventProducer;
import com.gwatcho.deliveryservice.repository.DeliveryRepository;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Slf4j
public class DeliveryService {
    private final DeliveryRepository deliveryRepository;
    private final DeliveryCompletedEventProducer deliveryCompletedEventProducer;

    /*
     * Temporary correlation for the case where payment.completed arrives
     * before order.created.
     *
     * IMPORTANT:
     * This map is not durable. A production implementation should persist
     * pending payment events so they survive service restarts.
     */
    private final Map<Long, PaymentCompletedEvent> pendingPayments = new ConcurrentHashMap<>();

    public DeliveryService(
            DeliveryRepository deliveryRepository, DeliveryCompletedEventProducer deliveryCompletedEventProducer) {
        this.deliveryRepository = deliveryRepository;
        this.deliveryCompletedEventProducer = deliveryCompletedEventProducer;
    }

    // =========================================================
    // ORDER.CREATED
    // =========================================================
    @Transactional
    public Delivery handleOrderCreated(OrderCreatedEvent event) {
        validateOrderCreatedEvent(event);

        Long orderId = event.orderId();

        log.info("Processing order.created: orderId={}, customerId={}", orderId, event.customerId());

        Delivery delivery = deliveryRepository.findByOrderId(orderId).orElseGet(
                () -> Delivery.builder().orderId(orderId).status(DeliveryStatus.CREATED).build());

        boolean isNew = delivery.getId() == null;

        // Populate order and delivery-address information.
        delivery.setCustomerId(event.customerId());
        delivery.setStreet(event.street());
        delivery.setPostalCode(event.postalCode());
        delivery.setCity(event.city());
        delivery.setCountry(event.country());
        delivery.setAmount(event.totalAmount());
        delivery.setCurrency(event.currency());

        if (delivery.getStatus() == null) {
            delivery.setStatus(DeliveryStatus.CREATED);
        }

        /*
         * If payment.completed arrived first, correlate it with this order.
         * Remove it from the map only after the database save succeeds.
         */
        PaymentCompletedEvent pendingPayment = pendingPayments.get(orderId);

        if (pendingPayment != null) {
            applyPaymentInformation(delivery, pendingPayment);
        }

        Delivery saved = deliveryRepository.save(delivery);

        if (pendingPayment != null) {
            pendingPayments.remove(orderId, pendingPayment);

            log.info("Pending payment correlated successfully: orderId={}, paymentId={}", saved.getOrderId(),
                    saved.getPaymentId());
        }

        log.info("{} delivery for orderId={}: deliveryId={}, status={}", isNew ? "Created" : "Updated", saved.getOrderId(),
                saved.getId(), saved.getStatus());

        return saved;
    }

    private void validateOrderCreatedEvent(OrderCreatedEvent event) {
        if (event == null) {
            throw new IllegalArgumentException("OrderCreatedEvent must not be null");
        }

        if (event.orderId() == null) {
            throw new IllegalArgumentException("OrderCreatedEvent orderId is required");
        }

        if (event.customerId() == null) {
            throw new IllegalArgumentException("OrderCreatedEvent customerId is required");
        }

        if (event.totalAmount() == null) {
            throw new IllegalArgumentException("OrderCreatedEvent totalAmount is required");
        }

        requireText(event.currency(), "currency");
        requireText(event.street(), "street");
        requireText(event.postalCode(), "postalCode");
        requireText(event.city(), "city");
        requireText(event.country(), "country");
    }

    private void requireText(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("OrderCreatedEvent " + fieldName + " is required");
        }
    }

    // =========================================================
    // PAYMENT.COMPLETED events
    // =========================================================
    @Transactional
    public Delivery handlePaymentCompleted(PaymentCompletedEvent event) {
        validatePaymentCompletedEvent(event);

        log.info("Processing payment.completed: paymentId={}, orderId={}, customerId={}", event.paymentId(),
                event.orderId(), event.customerId());

        Optional<Delivery> existingDelivery = deliveryRepository.findByOrderId(event.orderId());

        if (existingDelivery.isPresent()) {
            Delivery delivery = existingDelivery.get();

            applyPaymentInformation(delivery, event);

            if (delivery.getStatus() == null) {
                delivery.setStatus(DeliveryStatus.CREATED);
            }

            Delivery saved = deliveryRepository.save(delivery);

            // A duplicate payment event must not leave stale pending data.
            pendingPayments.remove(event.orderId());

            log.info("Payment information applied successfully: deliveryId={}, orderId={}, paymentId={}", saved.getId(),
                    saved.getOrderId(), saved.getPaymentId());

            return saved;
        }

        /*
         * Order information has not arrived yet.
         * Keep the payment temporarily for correlation in handleOrderCreated().
         */
        pendingPayments.put(event.orderId(), event);

        log.info("No delivery found for orderId={}. Payment stored as pending correlation.", event.orderId());

        /*
         * Return a transient object for compatibility with the existing method
         * signature. This object is NOT persisted and must not be treated as
         * an existing delivery.
         */
        return Delivery.builder()
                .orderId(event.orderId())
                .customerId(event.customerId())
                .paymentId(event.paymentId())
                .amount(event.amount())
                .currency(event.currency())
                .transactionId(event.transactionId())
                .status(DeliveryStatus.CREATED)
                .build();
    }

    private void validatePaymentCompletedEvent(PaymentCompletedEvent event) {
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

        requirePaymentText(event.currency(), "currency");
        requirePaymentText(event.transactionId(), "transactionId");
    }

    private void requirePaymentText(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("PaymentCompletedEvent " + fieldName + " is required");
        }
    }

    // =========================================================
    // PAYMENT INFORMATION
    // =========================================================
    private void applyPaymentInformation(Delivery delivery, PaymentCompletedEvent event) {
        delivery.setPaymentId(event.paymentId());
        delivery.setCustomerId(event.customerId());
        delivery.setAmount(event.amount());
        delivery.setCurrency(event.currency());
        delivery.setTransactionId(event.transactionId());
    }

    // =========================================================
    // GET DELIVERY
    // =========================================================
    @Transactional(readOnly = true)
    public Delivery getDelivery(Long deliveryId) {
        return deliveryRepository.findById(deliveryId)
                .orElseThrow(() -> new IllegalArgumentException("Delivery not found: " + deliveryId));
    }

    // =========================================================
    // GET BY ORDER ID
    // =========================================================
    @Transactional(readOnly = true)
    public Delivery getByOrderId(Long orderId) {
        return deliveryRepository.findByOrderId(orderId).orElseThrow(
                () -> new IllegalArgumentException("Delivery not found for order: " + orderId));
    }

    // =========================================================
    // FIND BY ORDER ID
    // =========================================================
    @Transactional(readOnly = true)
    public Optional<Delivery> findByOrderId(Long orderId) {
        return deliveryRepository.findByOrderId(orderId);
    }

    // =========================================================
    // UPDATE STATUS
    // =========================================================
    @Transactional
    public Delivery updateStatus(Long deliveryId, DeliveryStatus newStatus) {
        Delivery delivery = deliveryRepository.findById(deliveryId)
                .orElseThrow(() -> new IllegalArgumentException("Delivery not found: " + deliveryId));

        DeliveryStatus oldStatus = delivery.getStatus();
        validateTransition(oldStatus, newStatus);
        delivery.setStatus(newStatus);
        if (newStatus == DeliveryStatus.DELIVERED) {
            delivery.setDeliveredAt(Instant.now());
        }

        Delivery saved = deliveryRepository.save(delivery);
        log.info("Delivery status changed: deliveryId={}, orderId={}, {} -> {}", saved.getId(), saved.getOrderId(),
                oldStatus, saved.getStatus());

        if (newStatus == DeliveryStatus.DELIVERED) {
            publishDeliveryCompleted(saved);
        }

        return saved;
    }

    // =========================================================
    // DELIVERY COMPLETED EVENT
    // =========================================================
    private void publishDeliveryCompleted(Delivery delivery) {
        LocalDateTime localTime = delivery.getDeliveredAt().atZone(ZoneId.systemDefault()).toLocalDateTime();

        DeliveryCompletedEvent event = new DeliveryCompletedEvent(UUID.randomUUID().toString(), "DeliveryCompleted",
                delivery.getId(), delivery.getOrderId(), delivery.getCustomerId(), localTime);
        deliveryCompletedEventProducer.publish(event);
        log.info("delivery.completed published: deliveryId={}, orderId={}", delivery.getId(), delivery.getOrderId());
    }

    // =========================================================
    // STATUS TRANSITIONS
    // =========================================================
    private void validateTransition(DeliveryStatus current, DeliveryStatus next) {
        if (next == null) {
            throw new IllegalArgumentException("New delivery status must not be null");
        }

        if (current == next) {
            return;
        }

        if (current == null) {
            throw new IllegalStateException("Delivery status is not initialized");
        }

        boolean valid = switch (current) {
            case CREATED ->
                    next == DeliveryStatus.PREPARING
                            || next == DeliveryStatus.CANCELLED;

            case PREPARING ->
                    next == DeliveryStatus.SHIPPED
                            || next == DeliveryStatus.CANCELLED;

            case SHIPPED ->
                    next == DeliveryStatus.DELIVERED;

            case DELIVERED, CANCELLED -> false;
        };

        if (!valid) {
            throw new IllegalStateException(
                    "Invalid delivery status transition: "
                            + current
                            + " -> "
                            + next);
        }
    }

    @Transactional(readOnly = true)
    public List<Delivery> getAllDeliveries() {
        return deliveryRepository.findAllByOrderByCreatedAtDesc();
        /*return deliveryRepository.findAll(
                Sort.by(Sort.Direction.DESC, "createdAt")
        );*/
    }
}
