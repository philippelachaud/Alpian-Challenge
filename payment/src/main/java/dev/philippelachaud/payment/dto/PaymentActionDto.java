package dev.philippelachaud.payment.dto;

public record PaymentActionDto<T>(PaymentAction action, T payload) {
}
