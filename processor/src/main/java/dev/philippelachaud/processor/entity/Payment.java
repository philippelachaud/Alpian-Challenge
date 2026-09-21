package dev.philippelachaud.processor.entity;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.domain.Persistable;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;

import java.math.BigDecimal;
import java.time.Instant;

@Table("payment")
public record Payment(
        @Id String id,
        @Column("from_account_id") Long fromAccountId,
        @Column("to_account") String toAccount,
        BigDecimal amount,
        String currency,
        String reference,
        String status,
        @Column("created_at") Instant createdAt,
        @Transient boolean isNew
) implements Persistable<String> {
    // Interface used for entities that need custom control over whether they are considered
    // "new" or "existing" when saving to a database. It gives manual control over determining
    // whether an entity is "new" (needs INSERT) or "existing" (needs UPDATE) when saving to a database.
    // The Problem it solves is that Spring Data determines if an entity is new by checking if the
    // ID is null. However, this doesn't work well when you assign IDs manually (like UUIDs) before first save,
    // as the payment id is set to a UUID, but it should be INSERTed and not UPDATed
    public Payment(String id, Long fromAccountId, String toAccount, BigDecimal amount, 
                   String currency, String reference, String status, Instant createdAt) {
        this(id, fromAccountId, toAccount, amount, currency, reference, status, createdAt, true);
    }
    
    public Payment withStatus(String newStatus) {
        return new Payment(id, fromAccountId, toAccount, amount, currency, reference, newStatus, createdAt, false);
    }
    
    @Override
    public String getId() {
        return id;
    }
    
    @Override
    public boolean isNew() {
        return isNew;
    }
}
