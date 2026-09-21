package dev.philippelachaud.processor.route;

import dev.philippelachaud.processor.dto.PaymentMessage;
import dev.philippelachaud.processor.dto.PaymentStatus;
import dev.philippelachaud.processor.dto.PaymentStatusMessage;
import dev.philippelachaud.processor.service.PaymentService;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PaymentProcessorTest {

    @Mock
    private PaymentService paymentService;

    private PaymentProcessor paymentProcessor;
    private CamelContext camelContext;

    @BeforeEach
    void setUp() {
        paymentProcessor = new PaymentProcessor(paymentService);
        camelContext = new DefaultCamelContext();
    }

    @Test
    void shouldProcessCreatePaymentMessage() throws Exception {
        PaymentMessage paymentMessage = new PaymentMessage(
                "payment-123",
                "FROM_IBAN",
                "TO_IBAN",
                new BigDecimal("100.00"),
                "CHF",
                "Test payment",
                "PENDING",
                Instant.now(),
                "CREATE"
        );

        PaymentStatusMessage statusMessage = new PaymentStatusMessage(
                "payment-123",
                PaymentStatus.CREATED,
                Instant.now(),
                "Payment created successfully"
        );

        Exchange exchange = new DefaultExchange(camelContext);
        exchange.getIn().setBody(paymentMessage);

        when(paymentService.processPaymentMessage(any(PaymentMessage.class)))
                .thenReturn(Mono.just(statusMessage));

        paymentProcessor.process(exchange);

        verify(paymentService).processPaymentMessage(paymentMessage);
    }

    @Test
    void shouldProcessCancelPaymentMessage() throws Exception {
        PaymentMessage cancelMessage = new PaymentMessage(
                "payment-456",
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                "CANCEL"
        );

        PaymentStatusMessage statusMessage = new PaymentStatusMessage(
                "payment-456",
                PaymentStatus.CANCELLED,
                Instant.now(),
                "Payment cancelled successfully"
        );

        Exchange exchange = new DefaultExchange(camelContext);
        exchange.getIn().setBody(cancelMessage);

        when(paymentService.processPaymentMessage(any(PaymentMessage.class)))
                .thenReturn(Mono.just(statusMessage));

        paymentProcessor.process(exchange);

        verify(paymentService).processPaymentMessage(cancelMessage);
    }

    @Test
    void shouldHandleMultipleMessagesSequentially() throws Exception {
        PaymentMessage message1 = new PaymentMessage(
                "payment-1",
                "IBAN1",
                "IBAN2",
                BigDecimal.TEN,
                "CHF",
                "Payment 1",
                "PENDING",
                Instant.now(),
                "CREATE"
        );

        PaymentMessage message2 = new PaymentMessage(
                "payment-2",
                "IBAN3",
                "IBAN4",
                BigDecimal.ONE,
                "EUR",
                "Payment 2",
                "PENDING",
                Instant.now(),
                "CREATE"
        );

        PaymentStatusMessage status1 = new PaymentStatusMessage(
                "payment-1",
                PaymentStatus.CREATED,
                Instant.now(),
                "Created"
        );

        PaymentStatusMessage status2 = new PaymentStatusMessage(
                "payment-2",
                PaymentStatus.CREATED,
                Instant.now(),
                "Created"
        );

        Exchange exchange1 = new DefaultExchange(camelContext);
        exchange1.getIn().setBody(message1);

        Exchange exchange2 = new DefaultExchange(camelContext);
        exchange2.getIn().setBody(message2);

        when(paymentService.processPaymentMessage(message1)).thenReturn(Mono.just(status1));
        when(paymentService.processPaymentMessage(message2)).thenReturn(Mono.just(status2));

        paymentProcessor.process(exchange1);
        paymentProcessor.process(exchange2);

        verify(paymentService).processPaymentMessage(message1);
        verify(paymentService).processPaymentMessage(message2);
    }

    @Test
    void shouldHandleFailedStatusFromService() throws Exception {
        PaymentMessage paymentMessage = new PaymentMessage(
                "payment-789",
                "NONEXISTENT_IBAN",
                "TO_IBAN",
                new BigDecimal("100.00"),
                "CHF",
                "Test payment",
                "PENDING",
                Instant.now(),
                "CREATE"
        );

        PaymentStatusMessage failedStatus = new PaymentStatusMessage(
                "payment-789",
                PaymentStatus.FAILED,
                Instant.now(),
                "Account not found"
        );

        Exchange exchange = new DefaultExchange(camelContext);
        exchange.getIn().setBody(paymentMessage);

        when(paymentService.processPaymentMessage(any(PaymentMessage.class)))
                .thenReturn(Mono.just(failedStatus));

        paymentProcessor.process(exchange);

        verify(paymentService).processPaymentMessage(paymentMessage);
    }

    @Test
    void failureWhenServiceThrowsException() {
        PaymentMessage paymentMessage = new PaymentMessage(
                "payment-123",
                "FROM_IBAN",
                "TO_IBAN",
                new BigDecimal("100.00"),
                "CHF",
                "Test payment",
                "PENDING",
                Instant.now(),
                "CREATE"
        );

        Exchange exchange = new DefaultExchange(camelContext);
        exchange.getIn().setBody(paymentMessage);

        when(paymentService.processPaymentMessage(any(PaymentMessage.class)))
                .thenReturn(Mono.error(new RuntimeException("Database error")));

        assertThatThrownBy(() -> paymentProcessor.process(exchange))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Database error");

        verify(paymentService).processPaymentMessage(paymentMessage);
    }

    @Test
    void failureWhenMessageBodyIsNull() {
        Exchange exchange = new DefaultExchange(camelContext);
        exchange.getIn().setBody(null);

        when(paymentService.processPaymentMessage(null))
                .thenReturn(Mono.error(new NullPointerException("Payment message is null")));

        assertThatThrownBy(() -> paymentProcessor.process(exchange))
                .isInstanceOf(Exception.class);
    }

    @Test
    void failureWhenMessageBodyIsWrongType() {
        Exchange exchange = new DefaultExchange(camelContext);
        exchange.getIn().setBody("Not a PaymentMessage");

        when(paymentService.processPaymentMessage(null))
                .thenReturn(Mono.error(new IllegalArgumentException("Invalid message")));

        assertThatThrownBy(() -> paymentProcessor.process(exchange))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid message");
    }

    @Test
    void failureWhenMonoReturnsEmpty() {
        PaymentMessage paymentMessage = new PaymentMessage(
                "payment-123",
                "FROM_IBAN",
                "TO_IBAN",
                new BigDecimal("100.00"),
                "CHF",
                "Test payment",
                "PENDING",
                Instant.now(),
                "CREATE"
        );

        Exchange exchange = new DefaultExchange(camelContext);
        exchange.getIn().setBody(paymentMessage);

        when(paymentService.processPaymentMessage(any(PaymentMessage.class)))
                .thenReturn(Mono.empty());

        assertThatThrownBy(() -> paymentProcessor.process(exchange))
                .isInstanceOf(Exception.class);
    }

    @Test
    void failureWhenIllegalArgumentException() {
        PaymentMessage paymentMessage = new PaymentMessage(
                "payment-invalid",
                "INVALID",
                "INVALID",
                BigDecimal.ZERO,
                "XXX",
                "Invalid payment",
                "PENDING",
                Instant.now(),
                "CREATE"
        );

        Exchange exchange = new DefaultExchange(camelContext);
        exchange.getIn().setBody(paymentMessage);

        when(paymentService.processPaymentMessage(any(PaymentMessage.class)))
                .thenReturn(Mono.error(new IllegalArgumentException("Invalid payment data")));

        assertThatThrownBy(() -> paymentProcessor.process(exchange))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid payment data");
    }
}
