package dev.philippelachaud.processor.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

public record PaymentStatusMessage(
        String id,
        PaymentStatus status,
        @JsonProperty("updatedAt") Instant updatedAt,
        String message
) {
}
