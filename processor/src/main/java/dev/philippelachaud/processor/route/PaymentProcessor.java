package dev.philippelachaud.processor.route;

import dev.philippelachaud.processor.dto.PaymentMessage;
import dev.philippelachaud.processor.dto.PaymentStatusMessage;
import dev.philippelachaud.processor.service.PaymentService;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.Objects;

@Component
public class PaymentProcessor implements Processor {

    private static final Logger logger = LoggerFactory.getLogger(PaymentProcessor.class);

    private final PaymentService paymentService;

    public PaymentProcessor(PaymentService paymentService) {
        this.paymentService = paymentService;
    }

    /**
     * Process a payment message received in the given {@link Exchange}.
     *
     * @param exchange the Exchange object containing the payment message to process
     * @throws Exception if an error occurs while processing the payment message
     */
    @Override
    public void process(Exchange exchange) throws Exception {
        PaymentMessage paymentMessage = exchange.getIn().getBody(PaymentMessage.class);
        logger.info("Processing payment message: {}", paymentMessage);

        try {
            // SubscribeOn ensures the reactive chain runs on a thread that supports blocking operations
            PaymentStatusMessage statusMessage = paymentService
                    .processPaymentMessage(paymentMessage)
                    .subscribeOn(Schedulers.boundedElastic())
                    .block(Duration.ofSeconds(30));

            logger.info("Payment processed successfully: id={}, status={}", 
                    Objects.requireNonNull(statusMessage).id(), statusMessage.status());
        } catch (Exception e) {
            logger.error("Error processing payment message: {}", e.getMessage(), e);
            throw e; // Let Camel's error handling deal with it
        }
    }
}
