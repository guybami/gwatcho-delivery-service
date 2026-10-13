
package com.gwatcho.deliveryservice.event;

import java.math.BigDecimal;

public record OrderCreatedEvent(
        String eventId,
        String eventType,
        Long orderId,
        Long customerId,
        String status,
        BigDecimal totalAmount,
        String currency,
        String paymentMethod,
        String street,
        String postalCode,
        String city,
        String country
) {
}
