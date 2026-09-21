package dev.philippelachaud.processor.service;

import dev.philippelachaud.processor.dto.PaymentStatus;
import dev.philippelachaud.processor.dto.PaymentStatusMessage;
import org.apache.camel.ProducerTemplate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class StatusPublisherTest {

    @Mock
    private ProducerTemplate producerTemplate;

    private StatusPublisher statusPublisher;

    @BeforeEach
    void setUp() {
        statusPublisher = new StatusPublisher(producerTemplate);
    }

    // ==================== SUCCESS CASES ====================

    @Test
    void publishStatusChange_success_shouldSendToDirectRoute() {
        // Given
        PaymentStatusMessage statusMessage = new PaymentStatusMessage(
                "payment-123",
                PaymentStatus.CREATED,
                Instant.now(),
                "Payment created successfully"
        );

        doNothing().when(producerTemplate).sendBody(eq("direct:publish-status"), any());

        // When
        Mono<Void> result = statusPublisher.publishStatusChange(statusMessage);

        // Then
        StepVerifier.create(result)
                .verifyComplete();

        verify(producerTemplate).sendBody("direct:publish-status", statusMessage);
    }

    @Test
    void publishStatusChange_success_withCreatedStatus_shouldPublish() {
        // Given
        PaymentStatusMessage statusMessage = new PaymentStatusMessage(
                "payment-456",
                PaymentStatus.CREATED,
                Instant.now(),
                "Payment created successfully"
        );

        // When
        Mono<Void> result = statusPublisher.publishStatusChange(statusMessage);

        // Then
        StepVerifier.create(result)
                .verifyComplete();

        verify(producerTemplate).sendBody("direct:publish-status", statusMessage);
    }

    @Test
    void publishStatusChange_success_withExecutedStatus_shouldPublish() {
        // Given
        PaymentStatusMessage statusMessage = new PaymentStatusMessage(
                "payment-789",
                PaymentStatus.EXECUTED,
                Instant.now(),
                "Payment executed successfully"
        );

        // When
        Mono<Void> result = statusPublisher.publishStatusChange(statusMessage);

        // Then
        StepVerifier.create(result)
                .verifyComplete();

        verify(producerTemplate).sendBody("direct:publish-status", statusMessage);
    }

    @Test
    void publishStatusChange_success_withCancelledStatus_shouldPublish() {
        // Given
        PaymentStatusMessage statusMessage = new PaymentStatusMessage(
                "payment-101",
                PaymentStatus.CANCELLED,
                Instant.now(),
                "Payment cancelled successfully"
        );

        // When
        Mono<Void> result = statusPublisher.publishStatusChange(statusMessage);

        // Then
        StepVerifier.create(result)
                .verifyComplete();

        verify(producerTemplate).sendBody("direct:publish-status", statusMessage);
    }

    @Test
    void publishStatusChange_success_withFailedStatus_shouldPublish() {
        // Given
        PaymentStatusMessage statusMessage = new PaymentStatusMessage(
                "payment-202",
                PaymentStatus.FAILED,
                Instant.now(),
                "Insufficient balance"
        );

        // When
        Mono<Void> result = statusPublisher.publishStatusChange(statusMessage);

        // Then
        StepVerifier.create(result)
                .verifyComplete();

        verify(producerTemplate).sendBody("direct:publish-status", statusMessage);
    }

    @Test
    void publishStatusChange_success_multipleMessages_shouldPublishAll() {
        // Given
        PaymentStatusMessage message1 = new PaymentStatusMessage(
                "payment-1",
                PaymentStatus.CREATED,
                Instant.now(),
                "Created"
        );
        PaymentStatusMessage message2 = new PaymentStatusMessage(
                "payment-2",
                PaymentStatus.EXECUTED,
                Instant.now(),
                "Executed"
        );

        // When
        Mono<Void> result1 = statusPublisher.publishStatusChange(message1);
        Mono<Void> result2 = statusPublisher.publishStatusChange(message2);

        // Then
        StepVerifier.create(result1)
                .verifyComplete();
        StepVerifier.create(result2)
                .verifyComplete();

        verify(producerTemplate).sendBody("direct:publish-status", message1);
        verify(producerTemplate).sendBody("direct:publish-status", message2);
    }

    // ==================== FAILURE CASES ====================

    @Test
    void publishStatusChange_failure_whenCamelThrowsException_shouldPropagateError() {
        // Given
        PaymentStatusMessage statusMessage = new PaymentStatusMessage(
                "payment-123",
                PaymentStatus.CREATED,
                Instant.now(),
                "Payment created successfully"
        );

        doThrow(new RuntimeException("Kafka unavailable"))
                .when(producerTemplate).sendBody(eq("direct:publish-status"), any());

        // When
        Mono<Void> result = statusPublisher.publishStatusChange(statusMessage);

        // Then
        StepVerifier.create(result)
                .expectErrorMatches(throwable ->
                        throwable instanceof RuntimeException &&
                        throwable.getMessage().contains("Failed to publish status change")
                )
                .verify();

        verify(producerTemplate).sendBody("direct:publish-status", statusMessage);
    }

    @Test
    void publishStatusChange_failure_whenNetworkError_shouldPropagateError() {
        // Given
        PaymentStatusMessage statusMessage = new PaymentStatusMessage(
                "payment-456",
                PaymentStatus.FAILED,
                Instant.now(),
                "Network error"
        );

        doThrow(new RuntimeException("Connection timeout"))
                .when(producerTemplate).sendBody(eq("direct:publish-status"), any());

        // When
        Mono<Void> result = statusPublisher.publishStatusChange(statusMessage);

        // Then
        StepVerifier.create(result)
                .expectErrorMatches(throwable ->
                        throwable instanceof RuntimeException &&
                        throwable.getMessage().contains("Failed to publish status change")
                )
                .verify();
    }

    @Test
    void publishStatusChange_failure_whenProducerTemplateIsNull_shouldThrowNullPointerException() {
        // Given
        StatusPublisher publisherWithNullTemplate = new StatusPublisher(null);
        PaymentStatusMessage statusMessage = new PaymentStatusMessage(
                "payment-789",
                PaymentStatus.CREATED,
                Instant.now(),
                "Test"
        );

        // When
        Mono<Void> result = publisherWithNullTemplate.publishStatusChange(statusMessage);

        // Then
        StepVerifier.create(result)
                .expectError(RuntimeException.class)
                .verify();
    }

    @Test
    void publishStatusChange_failure_whenMessageIsNull_shouldFail() {
        // Given
        PaymentStatusMessage nullMessage = null;
        
        // Camel's sendBody will throw NullPointerException when trying to log null
        doThrow(new NullPointerException("Cannot invoke method on null object"))
                .when(producerTemplate).sendBody(eq("direct:publish-status"), isNull());

        // When
        Mono<Void> result = statusPublisher.publishStatusChange(nullMessage);

        // Then - should fail with RuntimeException wrapping the NPE
        StepVerifier.create(result)
                .expectErrorMatches(throwable ->
                        throwable instanceof RuntimeException &&
                        throwable.getMessage().contains("Failed to publish status change")
                )
                .verify();

        verify(producerTemplate).sendBody("direct:publish-status", nullMessage);
    }

    @Test
    void publishStatusChange_failure_whenUnexpectedError_shouldWrapInRuntimeException() {
        // Given
        PaymentStatusMessage statusMessage = new PaymentStatusMessage(
                "payment-999",
                PaymentStatus.CREATED,
                Instant.now(),
                "Test"
        );

        doThrow(new IllegalStateException("Invalid state"))
                .when(producerTemplate).sendBody(eq("direct:publish-status"), any());

        // When
        Mono<Void> result = statusPublisher.publishStatusChange(statusMessage);

        // Then
        StepVerifier.create(result)
                .expectErrorMatches(throwable ->
                        throwable instanceof RuntimeException &&
                        throwable.getMessage().contains("Failed to publish status change") &&
                        throwable.getCause() instanceof IllegalStateException
                )
                .verify();
    }
}
