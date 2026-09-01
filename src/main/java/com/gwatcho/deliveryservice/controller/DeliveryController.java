package com.gwatcho.deliveryservice.controller;

import com.gwatcho.deliveryservice.entity.Delivery;
import com.gwatcho.deliveryservice.entity.DeliveryStatus;
import com.gwatcho.deliveryservice.service.DeliveryService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/deliveries")
public class DeliveryController {
    private final DeliveryService deliveryService;

    public DeliveryController(DeliveryService deliveryService) {
        this.deliveryService = deliveryService;
    }

    @PutMapping("/{deliveryId}/status")
    public ResponseEntity<Delivery> updateStatus(
            @PathVariable Long deliveryId, @Valid @RequestBody UpdateDeliveryStatusRequest request) {
        Delivery delivery = deliveryService.updateStatus(deliveryId, request.status());

        return ResponseEntity.ok(delivery);
    }

    @GetMapping("/order/{orderId}")
    public ResponseEntity<Delivery> getByOrderId(
            @PathVariable Long orderId
    ) {
        return deliveryService.findByOrderId(orderId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    public record UpdateDeliveryStatusRequest(@NotNull DeliveryStatus status) {}
}

