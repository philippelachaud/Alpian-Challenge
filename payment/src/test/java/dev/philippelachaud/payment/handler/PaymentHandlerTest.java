package dev.philippelachaud.payment.handler;

import dev.philippelachaud.payment.dto.PaymentDto;
import dev.philippelachaud.payment.dto.PaymentStatus;
import dev.philippelachaud.payment.service.PaymentService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class PaymentHandlerTest {

    @Mock
    private PaymentService paymentService;

    @Mock
    private ServerRequest serverRequest;

    private PaymentHandler paymentHandler;

    @BeforeEach
    void setUp() {
        paymentHandler = new PaymentHandler(paymentService);
    }

    @Test
    void createPaymentSuccess() {
        PaymentDto paymentDto = new PaymentDto(
                "test-id",
                "CH9300762011623852957",
                "CH1234567890123456789",
                new BigDecimal("100.00"),
                "CHF",
                "Test payment",
                PaymentStatus.PENDING,
                Instant.now()
        );

        when(serverRequest.bodyToMono(PaymentDto.class)).thenReturn(Mono.just(paymentDto));
        when(paymentService.createPayment(any(PaymentDto.class))).thenReturn("test-payment-id-123");

        Mono<ServerResponse> responseMono = paymentHandler.createPayment(serverRequest);

        StepVerifier.create(responseMono)
                .assertNext(response -> {
                    assertThat(response.statusCode()).isEqualTo(HttpStatus.CREATED);
                })
                .verifyComplete();

        verify(paymentService).createPayment(paymentDto);
    }

    @Test
    void createPaymentSuccessWithLargeAmount() {
        PaymentDto paymentDto = new PaymentDto(
                "test-id",
                "FROM",
                "TO",
                new BigDecimal("999999.99"),
                "EUR",
                "Large payment",
                PaymentStatus.PENDING,
                Instant.now()
        );

        when(serverRequest.bodyToMono(PaymentDto.class)).thenReturn(Mono.just(paymentDto));
        when(paymentService.createPayment(any(PaymentDto.class))).thenReturn("large-payment-id-456");

        Mono<ServerResponse> responseMono = paymentHandler.createPayment(serverRequest);

        StepVerifier.create(responseMono)
                .assertNext(response -> assertThat(response.statusCode()).isEqualTo(HttpStatus.CREATED))
                .verifyComplete();
    }

    @Test
    void createPaymentFailure() {
        PaymentDto paymentDto = new PaymentDto(
                "test-id",
                "FROM",
                "TO",
                BigDecimal.TEN,
                "CHF",
                "Ref",
                PaymentStatus.PENDING,
                Instant.now()
        );

        when(serverRequest.bodyToMono(PaymentDto.class)).thenReturn(Mono.just(paymentDto));
        when(paymentService.createPayment(any(PaymentDto.class)))
                .thenThrow(new RuntimeException("Payment processing failed"));

        Mono<ServerResponse> responseMono = paymentHandler.createPayment(serverRequest);

        StepVerifier.create(responseMono)
                .assertNext(response -> {
                    assertThat(response.statusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
                })
                .verifyComplete();
    }

    @Test
    void createPaymentFailureWhenRequestBodyIsInvalid() {
        when(serverRequest.bodyToMono(PaymentDto.class))
                .thenReturn(Mono.error(new RuntimeException("Invalid JSON")));

        Mono<ServerResponse> responseMono = paymentHandler.createPayment(serverRequest);

        StepVerifier.create(responseMono)
                .assertNext(response -> {
                    assertThat(response.statusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
                })
                .verifyComplete();

        verify(paymentService, never()).createPayment(any());
    }


    @Test
    void cancelPaymentSuccess() {
        String paymentId = "test-payment-123";

        when(serverRequest.queryParam("id")).thenReturn(Optional.of(paymentId));
        when(paymentService.cancelPayment(paymentId)).thenReturn(Optional.empty());

        Mono<ServerResponse> responseMono = paymentHandler.cancelPayment(serverRequest);

        StepVerifier.create(responseMono)
                .assertNext(response -> {
                    assertThat(response.statusCode()).isEqualTo(HttpStatus.OK);
                })
                .verifyComplete();

        verify(paymentService).cancelPayment(paymentId);
    }

    @Test
    void cancelPaymentFailure() {
        String paymentId = "test-payment-123";

        when(serverRequest.queryParam("id")).thenReturn(Optional.of(paymentId));
        when(paymentService.cancelPayment(paymentId))
                .thenReturn(Optional.of("Payment cancellation failed"));

        Mono<ServerResponse> responseMono = paymentHandler.cancelPayment(serverRequest);

        StepVerifier.create(responseMono)
                .assertNext(response -> {
                    assertThat(response.statusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
                })
                .verifyComplete();
    }
}
