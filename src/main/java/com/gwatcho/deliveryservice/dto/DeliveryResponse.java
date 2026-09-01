package com.gwatcho.deliveryservice.dto;

import com.gwatcho.deliveryservice.entity.DeliveryStatus;

import java.math.BigDecimal;
import java.time.Instant;

public record DeliveryResponse(

        Long id,

        Long orderId,

        Long paymentId,

        Long customerId,

        DeliveryStatus status,

        BigDecimal amount,

        String currency,

        String transactionId,

        String street,

        String postalCode,

        String city,

        String country,

        Instant createdAt,

        Instant updatedAt,

        Instant deliveredAt
) {
}