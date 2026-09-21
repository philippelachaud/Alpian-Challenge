package dev.philippelachaud.payment.dto;

import java.math.BigDecimal;
import java.time.Instant;

public record PaymentDto(
        String id,
        String fromAccount,
        String toAccount,
        BigDecimal amount,
        String currency,
        String reference,
        PaymentStatus status,
        Instant createdAt
) {
    /**
     * Creates a new PaymentDto with the specified ID.
     */
    public PaymentDto withId(String newId) {
        return new PaymentDto(
                newId,
                this.fromAccount,
                this.toAccount,
                this.amount,
                this.currency,
                this.reference,
                this.status,
                this.createdAt
        );
    }
}
