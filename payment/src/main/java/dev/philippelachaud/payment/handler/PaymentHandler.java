package dev.philippelachaud.payment.handler;

import dev.philippelachaud.payment.dto.PaymentDto;
import dev.philippelachaud.payment.service.PaymentService;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.Mono;

@Component
public class PaymentHandler {
    final PaymentService paymentService;

    public PaymentHandler(PaymentService paymentService) {
        this.paymentService = paymentService;
    }

    /***
     * Creates a new payment (i.e.: send a CREATE message on Kafka) using the information from PaymentDto.
     * @param request The ServerRequest containing the PaymentDto object.
     * @return A Mono<ServerResponse> representing the response to the request, containing the payment ID.
     */
    public Mono<ServerResponse> createPayment(ServerRequest request) {
        return request.bodyToMono(PaymentDto.class)
                .flatMap(payment -> {
                    try {
                        String paymentId = paymentService.createPayment(payment);
                        return ServerResponse.status(HttpStatus.CREATED)
                                .bodyValue(java.util.Map.of("id", paymentId));
                    } catch (Exception e) {
                        return ServerResponse.badRequest()
                                .bodyValue(java.util.Map.of("error", e.getMessage()));
                    }
                })
                .onErrorResume(t -> ServerResponse.badRequest()
                        .bodyValue(java.util.Map.of("error", "Error creating payment: " + t.getMessage())));
    }

    /***
     * Cancels a payment (i.e.: send a CANCEL message on Kafka) using the provided payment ID.
     * @param request The ServerRequest containing the payment ID.
     * @return A Mono<ServerResponse> representing the response to the request. I.e.: OK if the payment was
     * canceled successfully, or an error message if the payment could not be canceled.
     */
    public Mono<ServerResponse> cancelPayment(ServerRequest request) {
        String id;
        try {
            id = request.queryParam("id").orElse("");
        } catch (Exception e) {
            return ServerResponse.badRequest().bodyValue("Error canceling payment");
        }

        return paymentService.cancelPayment(id)
                .map(error -> ServerResponse.badRequest().bodyValue(error))
                .orElse(ServerResponse.status(HttpStatus.OK).build());
    }
}
