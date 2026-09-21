package dev.philippelachaud.processor.service;

import dev.philippelachaud.processor.dto.PaymentMessage;
import dev.philippelachaud.processor.dto.PaymentStatus;
import dev.philippelachaud.processor.dto.PaymentStatusMessage;
import dev.philippelachaud.processor.entity.Account;
import dev.philippelachaud.processor.entity.Payment;
import dev.philippelachaud.processor.repository.AccountRepository;
import dev.philippelachaud.processor.repository.PaymentRepository;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;

import java.time.Instant;

@Service
public class PaymentService {

    private static final Logger logger = LoggerFactory.getLogger(PaymentService.class);

    private final PaymentRepository paymentRepository;
    private final AccountRepository accountRepository;
    private final StatusPublisher statusPublisher;
    private final TransactionalOperator transactionalOperator;

    public PaymentService(PaymentRepository paymentRepository, 
                         AccountRepository accountRepository, 
                         StatusPublisher statusPublisher,
                         TransactionalOperator transactionalOperator) {
        this.paymentRepository = paymentRepository;
        this.accountRepository = accountRepository;
        this.statusPublisher = statusPublisher;
        this.transactionalOperator = transactionalOperator;
    }

    /**
     * Create and publish a payment status message.
     *
     * @param paymentId the payment ID
     * @param status the payment status
     * @param message the status message
     * @return a {@code Mono<PaymentStatusMessage>} after publishing
     */
    private Mono<PaymentStatusMessage> publishStatusMessage(String paymentId, PaymentStatus status, String message) {
        PaymentStatusMessage statusMessage = new PaymentStatusMessage(paymentId, status, Instant.now(), message);
        return statusPublisher.publishStatusChange(statusMessage).thenReturn(statusMessage);
    }

    /**
     * Handle failures by logging and publishing a failure status message.
     *
     * @param paymentId the payment ID
     * @param errorMessage the error message to publish
     * @param error the throwable error (optional, can be null)
     * @param logMessage the log message format
     * @return a {@code Mono<PaymentStatusMessage>} with failure status
     */
    private Mono<PaymentStatusMessage> handleFailure(String paymentId, String errorMessage, Throwable error, String logMessage) {
        if (error != null) {
            logger.error(logMessage, error.getMessage(), error);
        } else {
            logger.error(logMessage, errorMessage);
        }
        return publishStatusMessage(paymentId, PaymentStatus.FAILED, errorMessage);
    }

    /**
     * Process a payment message. It determines the action to perform (e.g., "CREATE" or "CANCEL")
     * and delegates to the appropriate method.
     *
     * @param message the payment message containing details of the payment action and associated data
     * @return a {@code Mono<PaymentStatusMessage>} representing the result of the payment operation
     */
    public Mono<PaymentStatusMessage> processPaymentMessage(PaymentMessage message) {
        logger.info("Processing payment message: {}", message);

        Mono<PaymentStatusMessage> operation;
        if ("CREATE".equalsIgnoreCase(message.action())) {
            operation = createPayment(message);
        } else if ("CANCEL".equalsIgnoreCase(message.action())) {
            operation = cancelPayment(message);
        } else {
            operation = Mono.just(new PaymentStatusMessage(
                    message.id(),
                    PaymentStatus.FAILED,
                    Instant.now(),
                    "Unknown action: " + message.action()
            ));
        }
        
        // The operation needs to be executed in a transactional context
        return operation.as(transactionalOperator::transactional);
    }

    /**
     * Create a payment from the payment message.
     *
     * @param message the payment message
     * @return a reactive Mono containing the result of the payment operation, either a {@code PaymentStatusMessage}
     *         representing the success or failure of the payment, or an error in case of processing failure
     */
    private Mono<PaymentStatusMessage> createPayment(PaymentMessage message) {
        return accountRepository.findByIban(message.fromAccount())
                .switchIfEmpty(Mono.error(new IllegalArgumentException("Account not found: " + message.fromAccount())))
                .flatMap(account -> savePayment(message, account))
                .onErrorResume(IllegalArgumentException.class, e -> invalidPayment(message, e))
                .onErrorResume(e -> creationError(message, e));
    }

    /***
     * Handle creation errors during payment creation.
     *
     * @param message the payment message
     * @param e       the error that occurred
     * @return a reactive Mono containing the result of the payment operation, either a {@code PaymentStatusMessage}
     *         representing the failure of the payment, or an error in case of processing failure
     */
    private @NonNull Mono<PaymentStatusMessage> creationError(PaymentMessage message, Throwable e) {
        return handleFailure(message.id(), "Error creating payment: " + e.getMessage(), e, "Error creating payment: {}");
    }

    /***
     * Handle invalid payment error during payment creation.
     *
     * @param message the payment message
     * @param e       the error that occurred
     * @return a reactive Mono containing the result of the payment operation, either a {@code PaymentStatusMessage}
     *         representing the failure of the payment, or an error in case of processing failure
     */
    private @NonNull Mono<PaymentStatusMessage> invalidPayment(PaymentMessage message, IllegalArgumentException e) {
        return handleFailure(message.id(), e.getMessage(), null, "Invalid payment request: {}");
    }

    /**
     * Process and save the payment information while validating account balance
     * Publishes the status change of the payment after saving.
     *
     * @param message the {@link PaymentMessage} containing details about the payment to be processed
     * @param account the {@link Account} object representing the account making the payment
     * @return a {@link Mono} emitting the {@link PaymentStatusMessage} containing the payment status after processing
     */
    private @NonNull Mono<PaymentStatusMessage> savePayment(PaymentMessage message, Account account) {
        if (account.amount().compareTo(message.amount()) < 0) {
            String errorMsg = "Insufficient balance. Available: " + account.amount() + ", Required: " + message.amount();
            logger.warn("Payment {} rejected: insufficient balance in account {}", message.id(), account.iban());
            return publishStatusMessage(message.id(), PaymentStatus.FAILED, errorMsg);
        }

        Payment payment = new Payment(
                message.id(),
                account.id(),
                message.toAccount(),
                message.amount(),
                message.currency(),
                message.reference(),
                "CREATED",
                message.createdAt() != null ? message.createdAt() : Instant.now()
        );

        return paymentRepository.save(payment)
                .flatMap(this::paymentSuccess)
                .onErrorResume(DataIntegrityViolationException.class, e -> paymentAlreadyExists(message));
    }

    /**
     * Handle the case where a payment with the given ID already exists.
     *
     * @param message the payment message
     * @return a {@code Mono<PaymentStatusMessage>} containing the failure status message
     */
    private @NonNull Mono<PaymentStatusMessage> paymentAlreadyExists(PaymentMessage message) {
        logger.warn("Payment with ID {} already exists", message.id());
        return publishStatusMessage(message.id(), PaymentStatus.FAILED, "Payment ID already exists - duplicate payment prevented");
    }

    /**
     * Process a successful payment and publishes it to the status topic.
     *
     * @param savedPayment the payment object that was successfully saved
     * @return a {@link Mono} emitting the constructed {@link PaymentStatusMessage} after publishing the status change
     */
    private @NonNull Mono<PaymentStatusMessage> paymentSuccess(Payment savedPayment) {
        logger.info("Payment created successfully: {}", savedPayment.id());
        // Publish status change to status-topic
        return publishStatusMessage(savedPayment.id(), PaymentStatus.CREATED, "Payment created successfully");
    }

    private Mono<PaymentStatusMessage> cancelPayment(PaymentMessage message) {
        return paymentRepository.findById(message.id())
                .switchIfEmpty(Mono.error(new IllegalArgumentException("Payment not found: " + message.id())))
                .flatMap(payment -> cancelPayment(message, payment))
                .onErrorResume(IllegalArgumentException.class, e -> invalidCancel(e, message))
                .onErrorResume(e -> handleFailure(message.id(), "Error cancelling payment: " + e.getMessage(), e, "Error cancelling payment: {}"));
    }

    /**
     * Handle an invalid cancellation, creating a failure status message, and publishing the status change.
     *
     * @param e the exception representing the invalid cancellation reason
     * @param message the original payment message
     * @return a {@link Mono} that emits the created {@link PaymentStatusMessage} indicating the failure status
     */
    private @NonNull Mono<PaymentStatusMessage> invalidCancel(IllegalArgumentException e, PaymentMessage message) {
        return handleFailure(message.id(), e.getMessage(), null, "Invalid cancellation request: {}");
    }

    /**
     * Cancel a payment by updating its status based on the current status of the payment. Publishes a status
     * change message
     *
     * @param message the payment message
     * @param payment the payment object to be cancelled
     * @return a {@code Mono<PaymentStatusMessage>} representing the status of the operation,
     *         either with a failure state if cancellation is not allowable or with a success
     *         state if the payment is successfully cancelled
     */
    private @NonNull Mono<PaymentStatusMessage> cancelPayment(PaymentMessage message, Payment payment) {
        if ("EXECUTED".equals(payment.status())) {
            return publishStatusMessage(message.id(), PaymentStatus.FAILED, "Cannot cancel executed payment");
        }

        if ("CANCELLED".equals(payment.status())) {
            return publishStatusMessage(message.id(), PaymentStatus.CANCELLED, "Payment already cancelled");
        }

        Payment updatedPayment = payment.withStatus("CANCELLED");
        return paymentRepository.save(updatedPayment)
                .flatMap(savedPayment -> {
                    logger.info("Payment cancelled successfully: {}", savedPayment.id());
                    // Publish status change to status-topic
                    return publishStatusMessage(savedPayment.id(), PaymentStatus.CANCELLED, "Payment cancelled successfully");
                });
    }

    /**
     * Executes the payment with the given payment ID. If the payment is not found
     * or not in the correct status, or if an error occurs, an error message is generated and handled.
     *
     * @param paymentId the ID of the payment to be executed
     * @return a Mono containing the status message of the executed payment or an error if the operation fails
     */
    public Mono<PaymentStatusMessage> executePayment(String paymentId) {
        return paymentRepository.findById(paymentId)
                .switchIfEmpty(Mono.error(new IllegalArgumentException("Payment not found: " + paymentId)))
                .flatMap(payment -> {
                    if (!"CREATED".equals(payment.status())) {
                        String errorMsg = "Payment is not in CREATED status, current status: " + payment.status();
                        return publishStatusMessage(paymentId, PaymentStatus.FAILED, errorMsg);
                    }

                    Payment updatedPayment = payment.withStatus("EXECUTED");
                    return paymentRepository.save(updatedPayment)
                            .flatMap(savedPayment -> {
                                logger.info("Payment executed successfully: {}", savedPayment.id());
                                // Publish status change to status-topic
                                return publishStatusMessage(savedPayment.id(), PaymentStatus.EXECUTED, "Payment executed successfully");
                            });
                })
                .onErrorResume(e -> handleFailure(paymentId, "Error executing payment: " + e.getMessage(), e, "Error executing payment: {}"));
    }
}
