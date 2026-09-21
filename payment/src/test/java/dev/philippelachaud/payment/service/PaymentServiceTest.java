package dev.philippelachaud.payment.service;

import dev.philippelachaud.payment.dto.PaymentAction;
import dev.philippelachaud.payment.dto.PaymentActionDto;
import dev.philippelachaud.payment.dto.PaymentDto;
import dev.philippelachaud.payment.dto.PaymentStatus;
import org.apache.camel.CamelExecutionException;
import org.apache.camel.ProducerTemplate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class PaymentServiceTest {

    @Mock
    private ProducerTemplate producerTemplate;

    private PaymentService paymentService;

    @BeforeEach
    void setUp() {
        paymentService = new PaymentService(producerTemplate);
    }


    @Test
    void createPaymentSuccess() {
        PaymentDto paymentDto = new PaymentDto(
                null,
                "CH9300762011623852957",
                "CH1234567890123456789",
                new BigDecimal("100.00"),
                "CHF",
                "Test payment",
                PaymentStatus.PENDING,
                Instant.now()
        );

        String result = paymentService.createPayment(paymentDto);

        assertThat(result).isNotNull(); // Returns the payment ID
        assertThat(paymentDto.id()).isNull(); // Original DTO is unchanged

        ArgumentCaptor<PaymentActionDto> captor = ArgumentCaptor.forClass(PaymentActionDto.class);
        verify(producerTemplate).sendBody(eq("direct:createPayment"), captor.capture());

        PaymentActionDto<PaymentDto> capturedAction = captor.getValue();
        assertThat(capturedAction.action()).isEqualTo(PaymentAction.CREATE);
        assertThat(capturedAction.payload().id()).isNotNull();
        assertThat(capturedAction.payload().id()).isEqualTo(result);
    }

    @Test
    void createPaymentSuccessGenerateUniqueIds() {
        PaymentDto payment1 = new PaymentDto(null, "FROM1", "TO1", BigDecimal.TEN, "CHF", "Ref1", PaymentStatus.PENDING, Instant.now());
        PaymentDto payment2 = new PaymentDto(null, "FROM2", "TO2", BigDecimal.ONE, "CHF", "Ref2", PaymentStatus.PENDING, Instant.now());

        String id1 = paymentService.createPayment(payment1);
        String id2 = paymentService.createPayment(payment2);

        assertThat(id1).isNotEqualTo(id2);
        verify(producerTemplate, times(2)).sendBody(eq("direct:createPayment"), any(PaymentActionDto.class));
    }

    @Test
    void cancelPaymentSuccess() {
        String paymentId = "test-payment-123";

        Optional<String> result = paymentService.cancelPayment(paymentId);

        assertThat(result).isEmpty(); // Success case returns empty Optional

        ArgumentCaptor<PaymentActionDto> captor = ArgumentCaptor.forClass(PaymentActionDto.class);
        verify(producerTemplate).sendBody(eq("direct:createPayment"), captor.capture());

        PaymentActionDto capturedAction = captor.getValue();
        assertThat(capturedAction.action()).isEqualTo(PaymentAction.CANCEL);
    }

    @Test
    void createPaymentFailure() {
        PaymentDto paymentDto = new PaymentDto(
                null,
                "CH9300762011623852957",
                "CH1234567890123456789",
                new BigDecimal("100.00"),
                "CHF",
                "Test payment",
                PaymentStatus.PENDING,
                Instant.now()
        );

        doThrow(new CamelExecutionException("Kafka unavailable", null))
                .when(producerTemplate).sendBody(eq("direct:createPayment"), any(PaymentActionDto.class));

        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            paymentService.createPayment(paymentDto);
        });
        
        assertThat(exception.getMessage()).isEqualTo("Error when publishing payment");
        assertThat(paymentDto.id()).isNull();

        verify(producerTemplate).sendBody(eq("direct:createPayment"), any(PaymentActionDto.class));
    }


    @Test
    void cancelPaymentFailure() {
        String paymentId = "test-payment-123";

        doThrow(new CamelExecutionException("Kafka unavailable", null))
                .when(producerTemplate).sendBody(eq("direct:createPayment"), any(PaymentActionDto.class));

        Optional<String> result = paymentService.cancelPayment(paymentId);

        assertThat(result).isPresent();
        assertThat(result.get()).isEqualTo("Error when publishing payment");

        verify(producerTemplate).sendBody(eq("direct:createPayment"), any(PaymentActionDto.class));
    }
}
