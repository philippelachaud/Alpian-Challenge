package dev.philippelachaud.processor.entity;

import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import java.math.BigDecimal;

@Table("account")
public record Account(
        @Id Long id,
        String iban,
        BigDecimal amount,
        String currency
) {
}
