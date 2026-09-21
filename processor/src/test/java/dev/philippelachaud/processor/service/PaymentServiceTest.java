package dev.philippelachaud.processor.service;

import dev.philippelachaud.processor.dto.PaymentMessage;
import dev.philippelachaud.processor.dto.PaymentStatus;
import dev.philippelachaud.processor.dto.PaymentStatusMessage;
import dev.philippelachaud.processor.entity.Account;
import dev.philippelachaud.processor.entity.Payment;
import dev.philippelachaud.processor.repository.AccountRepository;
import dev.philippelachaud.processor.repository.PaymentRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class PaymentServiceTest {

    @Mock
    private PaymentRepository paymentRepository;

    @Mock
    private AccountRepository accountRepository;

    @Mock
    private StatusPublisher statusPublisher;

    @Mock
    private TransactionalOperator transactionalOperator;

    private PaymentService paymentService;

    @BeforeEach
    void setUp() {
        when(transactionalOperator.transactional(any(Mono.class))).
                thenAnswer(invocation -> invocation.getArgument(0));
        
        paymentService = new PaymentService(paymentRepository, accountRepository, statusPublisher, transactionalOperator);
        when(statusPublisher.publishStatusChange(any())).thenReturn(Mono.empty());
    }

    @Test
    void shouldSavePaymentAndPublishCreatedStatus() {
        PaymentMessage message = new PaymentMessage(
                "payment-123",
                "CH9300762011623852957",
                "CH1234567890123456789",
                new BigDecimal("100.00"),
                "CHF",
                "Test payment",
                "PENDING",
                Instant.now(),
                "CREATE"
        );

        Account account = new Account(1L, "CH9300762011623852957", new BigDecimal("500.00"), "CHF");
        Payment savedPayment = new Payment(
                "payment-123",
                1L,
                "CH1234567890123456789",
                new BigDecimal("100.00"),
                "CHF",
                "Test payment",
                "CREATED",
                message.createdAt()
        );

        when(accountRepository.findByIban("CH9300762011623852957")).thenReturn(Mono.just(account));
        when(paymentRepository.save(any(Payment.class))).thenReturn(Mono.just(savedPayment));

        Mono<PaymentStatusMessage> result = paymentService.processPaymentMessage(message);

        StepVerifier.create(result)
                .assertNext(statusMessage -> {
                    assertThat(statusMessage.id()).isEqualTo("payment-123");
                    assertThat(statusMessage.status()).isEqualTo(PaymentStatus.CREATED);
                    assertThat(statusMessage.message()).isEqualTo("Payment created successfully");
                })
                .verifyComplete();

        verify(accountRepository).findByIban("CH9300762011623852957");
        verify(paymentRepository).save(any(Payment.class));
        verify(statusPublisher).publishStatusChange(any(PaymentStatusMessage.class));
    }

    @Test
    void createPaymentWithExactBalanceMatch() {
        PaymentMessage message = new PaymentMessage(
                "payment-123",
                "CH9300762011623852957",
                "CH1234567890123456789",
                new BigDecimal("100.00"),
                "CHF",
                "Test payment",
                "PENDING",
                Instant.now(),
                "CREATE"
        );

        Account account = new Account(1L, "CH9300762011623852957", new BigDecimal("100.00"), "CHF");
        Payment savedPayment = new Payment(
                "payment-123",
                1L,
                "CH1234567890123456789",
                new BigDecimal("100.00"),
                "CHF",
                "Exact balance payment",
                "CREATED",
                message.createdAt()
        );

        when(accountRepository.findByIban("CH9300762011623852957")).thenReturn(Mono.just(account));
        when(paymentRepository.save(any(Payment.class))).thenReturn(Mono.just(savedPayment));

        Mono<PaymentStatusMessage> result = paymentService.processPaymentMessage(message);

        StepVerifier.create(result)
                .assertNext(statusMessage -> {
                    assertThat(statusMessage.status()).isEqualTo(PaymentStatus.CREATED);
                })
                .verifyComplete();
    }

    @Test
    void createPaymentWithNullCreatedAt() {
        PaymentMessage message = new PaymentMessage(
                "payment-123",
                "CH9300762011623852957",
                "CH1234567890123456789",
                new BigDecimal("50.00"),
                "CHF",
                "Test payment",
                "PENDING",
                null, // null createdAt
                "CREATE"
        );

        Account account = new Account(1L, "CH9300762011623852957", new BigDecimal("500.00"), "CHF");
        
        when(accountRepository.findByIban("CH9300762011623852957")).thenReturn(Mono.just(account));
        when(paymentRepository.save(any(Payment.class))).thenAnswer(invocation -> {
            Payment payment = invocation.getArgument(0);
            assertThat(payment.createdAt()).isNotNull();
            return Mono.just(payment);
        });

        Mono<PaymentStatusMessage> result = paymentService.processPaymentMessage(message);

        StepVerifier.create(result)
                .assertNext(statusMessage -> {
                    assertThat(statusMessage.status()).isEqualTo(PaymentStatus.CREATED);
                })
                .verifyComplete();
    }

    @Test
    void failureWhenAccountNotFound() {
        PaymentMessage message = new PaymentMessage(
                "payment-123",
                "NONEXISTENT_IBAN",
                "CH1234567890123456789",
                new BigDecimal("100.00"),
                "CHF",
                "Test payment",
                "PENDING",
                Instant.now(),
                "CREATE"
        );

        when(accountRepository.findByIban("NONEXISTENT_IBAN")).thenReturn(Mono.empty());

        Mono<PaymentStatusMessage> result = paymentService.processPaymentMessage(message);

        StepVerifier.create(result)
                .assertNext(statusMessage -> {
                    assertThat(statusMessage.id()).isEqualTo("payment-123");
                    assertThat(statusMessage.status()).isEqualTo(PaymentStatus.FAILED);
                    assertThat(statusMessage.message()).contains("Account not found");
                })
                .verifyComplete();

        verify(accountRepository).findByIban("NONEXISTENT_IBAN");
        verify(paymentRepository, never()).save(any());
        verify(statusPublisher).publishStatusChange(any(PaymentStatusMessage.class));
    }

    @Test
    void failureWhenInsufficientBalance() {
        PaymentMessage message = new PaymentMessage(
                "payment-123",
                "CH9300762011623852957",
                "CH1234567890123456789",
                new BigDecimal("1000.00"),
                "CHF",
                "Large payment",
                "PENDING",
                Instant.now(),
                "CREATE"
        );

        Account account = new Account(1L, "CH9300762011623852957", new BigDecimal("100.00"), "CHF");

        when(accountRepository.findByIban("CH9300762011623852957")).thenReturn(Mono.just(account));

        Mono<PaymentStatusMessage> result = paymentService.processPaymentMessage(message);

        StepVerifier.create(result)
                .assertNext(statusMessage -> {
                    assertThat(statusMessage.id()).isEqualTo("payment-123");
                    assertThat(statusMessage.status()).isEqualTo(PaymentStatus.FAILED);
                    assertThat(statusMessage.message()).contains("Insufficient balance");
                    assertThat(statusMessage.message()).contains("Available: 100.00");
                    assertThat(statusMessage.message()).contains("Required: 1000.00");
                })
                .verifyComplete();

        verify(accountRepository).findByIban("CH9300762011623852957");
        verify(paymentRepository, never()).save(any());
        verify(statusPublisher).publishStatusChange(any(PaymentStatusMessage.class));
    }

    @Test
    void failureWhenDuplicatePaymentId() {
        PaymentMessage message = new PaymentMessage(
                "payment-123",
                "CH9300762011623852957",
                "CH1234567890123456789",
                new BigDecimal("100.00"),
                "CHF",
                "Test payment",
                "PENDING",
                Instant.now(),
                "CREATE"
        );

        Account account = new Account(1L, "CH9300762011623852957", new BigDecimal("500.00"), "CHF");

        when(accountRepository.findByIban("CH9300762011623852957")).thenReturn(Mono.just(account));
        when(paymentRepository.save(any(Payment.class)))
                .thenReturn(Mono.error(new DataIntegrityViolationException("Duplicate key")));

        Mono<PaymentStatusMessage> result = paymentService.processPaymentMessage(message);

        StepVerifier.create(result)
                .assertNext(statusMessage -> {
                    assertThat(statusMessage.id()).isEqualTo("payment-123");
                    assertThat(statusMessage.status()).isEqualTo(PaymentStatus.FAILED);
                    assertThat(statusMessage.message()).contains("Payment ID already exists");
                })
                .verifyComplete();

        verify(statusPublisher, times(1)).publishStatusChange(any(PaymentStatusMessage.class));
    }

    @Test
    void failureWhenDatabaseError() {
        PaymentMessage message = new PaymentMessage(
                "payment-123",
                "CH9300762011623852957",
                "CH1234567890123456789",
                new BigDecimal("100.00"),
                "CHF",
                "Test payment",
                "PENDING",
                Instant.now(),
                "CREATE"
        );

        Account account = new Account(1L, "CH9300762011623852957", new BigDecimal("500.00"), "CHF");

        when(accountRepository.findByIban("CH9300762011623852957")).thenReturn(Mono.just(account));
        when(paymentRepository.save(any(Payment.class)))
                .thenReturn(Mono.error(new RuntimeException("Database connection failed")));

        Mono<PaymentStatusMessage> result = paymentService.processPaymentMessage(message);

        StepVerifier.create(result)
                .assertNext(statusMessage -> {
                    assertThat(statusMessage.id()).isEqualTo("payment-123");
                    assertThat(statusMessage.status()).isEqualTo(PaymentStatus.FAILED);
                    assertThat(statusMessage.message()).contains("Error creating payment");
                })
                .verifyComplete();
    }

    @Test
    void cancelPaymentSuccess() {
        PaymentMessage message = new PaymentMessage(
                "payment-123",
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                "CANCEL"
        );

        Payment existingPayment = new Payment(
                "payment-123",
                1L,
                "CH1234567890123456789",
                new BigDecimal("100.00"),
                "CHF",
                "Test payment",
                "CREATED",
                Instant.now()
        );

        Payment cancelledPayment = existingPayment.withStatus("CANCELLED");

        when(paymentRepository.findById("payment-123")).thenReturn(Mono.just(existingPayment));
        when(paymentRepository.save(any(Payment.class))).thenReturn(Mono.just(cancelledPayment));

        Mono<PaymentStatusMessage> result = paymentService.processPaymentMessage(message);

        StepVerifier.create(result)
                .assertNext(statusMessage -> {
                    assertThat(statusMessage.id()).isEqualTo("payment-123");
                    assertThat(statusMessage.status()).isEqualTo(PaymentStatus.CANCELLED);
                    assertThat(statusMessage.message()).isEqualTo("Payment cancelled successfully");
                })
                .verifyComplete();

        verify(paymentRepository).findById("payment-123");
        verify(paymentRepository).save(any(Payment.class));
        verify(statusPublisher).publishStatusChange(any(PaymentStatusMessage.class));
    }

    @Test
    void cancelPaymentFailure() {
        PaymentMessage message = new PaymentMessage(
                "nonexistent-payment",
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                "CANCEL"
        );

        when(paymentRepository.findById("nonexistent-payment")).thenReturn(Mono.empty());

        Mono<PaymentStatusMessage> result = paymentService.processPaymentMessage(message);

        StepVerifier.create(result)
                .assertNext(statusMessage -> {
                    assertThat(statusMessage.id()).isEqualTo("nonexistent-payment");
                    assertThat(statusMessage.status()).isEqualTo(PaymentStatus.FAILED);
                    assertThat(statusMessage.message()).contains("Payment not found");
                })
                .verifyComplete();

        verify(paymentRepository).findById("nonexistent-payment");
        verify(paymentRepository, never()).save(any());
    }
}
