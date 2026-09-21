package dev.philippelachaud.processor.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;
import java.time.Instant;

public record PaymentMessage(
        String id,
        @JsonProperty("fromAccount") String fromAccount,
        @JsonProperty("toAccount") String toAccount,
        BigDecimal amount,
        String currency,
        String reference,
        String status,
        @JsonProperty("createdAt") Instant createdAt,
        String action // CREATE or CANCEL
) {
}
