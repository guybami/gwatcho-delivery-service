package com.gwatcho.deliveryservice.service;

import com.gwatcho.deliveryservice.entity.Delivery;
import com.gwatcho.deliveryservice.entity.DeliveryStatus;
import com.gwatcho.deliveryservice.event.DeliveryCompletedEvent;
import com.gwatcho.deliveryservice.event.PaymentCompletedEvent;
import com.gwatcho.deliveryservice.kafka.DeliveryCompletedEventProducer;
import com.gwatcho.deliveryservice.repository.DeliveryRepository;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Slf4j
public class DeliveryService {
    private final DeliveryRepository deliveryRepository;

    /*
     * Payment events may arrive before the corresponding order information.
     *
     * In that case we keep the payment temporarily in memory and wait for
     * the order information before creating a persistent Delivery.
     */
    private final Map<Long, PaymentCompletedEvent> pendingPayments = new ConcurrentHashMap<>();

    private final DeliveryCompletedEventProducer deliveryCompletedEventProducer;

    public DeliveryService(
            DeliveryRepository deliveryRepository, DeliveryCompletedEventProducer deliveryCompletedEventProducer) {
        this.deliveryRepository = deliveryRepository;
        this.deliveryCompletedEventProducer = deliveryCompletedEventProducer;
    }

    // =========================================================
    // PAYMENT.COMPLETED
    // =========================================================

    @Transactional
    public Delivery handlePaymentCompleted(PaymentCompletedEvent event) {
        log.info("Processing payment.completed: paymentId={}, orderId={}, customerId={}", event.paymentId(), event.orderId(),
                event.customerId());

        Optional<Delivery> existingDelivery = deliveryRepository.findByOrderId(event.orderId());

        if (existingDelivery.isPresent()) {
            Delivery delivery = existingDelivery.get();

            log.info("Delivery already exists: deliveryId={}, orderId={}", delivery.getId(), delivery.getOrderId());

            applyPaymentInformation(delivery, event);

            if (delivery.getStatus() == null) {
                delivery.setStatus(DeliveryStatus.CREATED);
            }

            Delivery saved = deliveryRepository.save(delivery);

            log.info("Payment information applied successfully: deliveryId={}, orderId={}, status={}", saved.getId(),
                    saved.getOrderId(), saved.getStatus());

            return saved;
        }

        pendingPayments.put(event.orderId(), event);

        log.info("No Delivery found for orderId={}. Payment stored as pending correlation.", event.orderId());

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
    // INFORMATION CHECKS
    // =========================================================

    private boolean hasOrderInformation(Delivery delivery) {
        return delivery.getCustomerId() != null && delivery.getStreet() != null && !delivery.getStreet().isBlank()
                && delivery.getPostalCode() != null && !delivery.getPostalCode().isBlank() && delivery.getCity() != null
                && !delivery.getCity().isBlank() && delivery.getCountry() != null && !delivery.getCountry().isBlank();
    }

    private boolean hasPaymentInformation(Delivery delivery) {
        return delivery.getPaymentId() != null && delivery.getAmount() != null && delivery.getCurrency() != null
                && !delivery.getCurrency().isBlank() && delivery.getTransactionId() != null
                && !delivery.getTransactionId().isBlank();
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
    // UPDATE STATUS
    // =========================================================

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
                oldStatus, newStatus);

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
    // FIND BY ORDER ID
    // =========================================================

    public Optional<Delivery> findByOrderId(Long orderId) {
        return deliveryRepository.findByOrderId(orderId);
    }

    // =========================================================
    // STATUS TRANSITIONS
    // =========================================================

    private void validateTransition(DeliveryStatus current, DeliveryStatus next) {
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

            case DELIVERED,
                 CANCELLED ->
                    false;
        };

        if (!valid) {

            throw new IllegalStateException(
                    "Invalid delivery status transition: "
                            + current
                            + " -> "
                            + next);
        }
    }
}
