package com.gwatcho.deliveryservice.event;

import java.math.BigDecimal;

public record OrderItemEvent(
        Long productId,
        String sku,
        String productName,
        BigDecimal unitPrice,
        Integer quantity,
        BigDecimal lineTotal
) {
}