package com.gwatcho.deliveryservice.event;

import java.time.LocalDateTime;

public record DeliveryCompletedEvent(
        String eventId,
        String eventType,
        Long deliveryId,
        Long orderId,
        Long customerId,
        LocalDateTime deliveredAt
) {
}
