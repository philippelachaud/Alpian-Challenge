package dev.philippelachaud.payment.service;

import dev.philippelachaud.payment.dto.CancelPaymentDto;
import dev.philippelachaud.payment.dto.PaymentAction;
import dev.philippelachaud.payment.dto.PaymentActionDto;
import dev.philippelachaud.payment.dto.PaymentDto;
import org.apache.camel.CamelExecutionException;
import org.apache.camel.ProducerTemplate;
import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.UUID;

@Service
public class PaymentService {
    final ProducerTemplate producerTemplate;
    final String endPointUri = "direct:createPayment";

    public PaymentService(ProducerTemplate producerTemplate) {
        this.producerTemplate = producerTemplate;
    }

    /**
     * Create a new payment by generating a unique payment ID, associating it with the provided
     * PaymentDto, and sending a CREATE action to the payment processing endpoint.
     *
     * @param paymentDto The data transfer object containing the payment details
     * @return The unique identifier for the created payment.
     * @throws RuntimeException If an error occurs while publishing the payment action.
     */
    public String createPayment(PaymentDto paymentDto) {
        // Generate the payment ID
        String paymentId = UUID.randomUUID().toString();
        // Create a new PaymentDto with the generated ID (as records are immutable)
        PaymentDto paymentWithId = paymentDto.withId(paymentId);
        // Send the payment action (i.e.: CREATE, CANCEL) to the endpoint
        Optional<String> error = processPaymentAction(new PaymentActionDto<>(PaymentAction.CREATE, paymentWithId));
        if (error.isPresent()) {
            throw new RuntimeException(error.get());
        }
        return paymentId;
    }

    /***
     * Cancel a payment by sending a CANCEL action to the endpoint.
     *
     * @param id The unique identifier of the payment to cancel.
     * @return An Optional containing the error message if an error occurs, or an empty Optional if the cancellation
     * is successful.
     */
    public Optional<String> cancelPayment(String id) {
        return processPaymentAction(new PaymentActionDto<>(PaymentAction.CANCEL, new CancelPaymentDto(id)));
    }

    /***
     * Process a payment action by publishing it to the endpoint.
     *
     * @param paymentActionDto The payment action data transfer object.
     * @param <T>              The type of the payment action data.
     * @return An Optional containing the error message if an error occurs, or an empty Optional if the action is
     * processed successfully.
     */
    private <T> Optional<String> processPaymentAction(PaymentActionDto<T> paymentActionDto) {
        try {
            // Publish the payment action
            producerTemplate.sendBody(endPointUri, paymentActionDto);
        } catch (CamelExecutionException e) {
            return Optional.of("Error when publishing payment");
        }

        return Optional.empty();
    }
}
