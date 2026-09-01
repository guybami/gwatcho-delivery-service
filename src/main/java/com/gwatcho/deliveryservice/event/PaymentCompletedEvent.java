package com.gwatcho.deliveryservice.event;

import com.gwatcho.deliveryservice.dto.DeliveryAddress;

import java.math.BigDecimal;

public record PaymentCompletedEvent(

        Long paymentId,
        Long orderId,
        Long customerId,
        BigDecimal amount,
        String currency,
        String transactionId,
        DeliveryAddress deliveryAddress
) {
}