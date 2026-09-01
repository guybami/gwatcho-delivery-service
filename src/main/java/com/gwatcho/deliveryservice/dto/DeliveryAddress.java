package com.gwatcho.deliveryservice.dto;


public record DeliveryAddress (
    String street,
    String postalCode,
    String city,
    String country
    ) {
}