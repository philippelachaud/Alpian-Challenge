package dev.philippelachaud.processor.service;

import dev.philippelachaud.processor.dto.PaymentStatusMessage;
import org.apache.camel.ProducerTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
public class StatusPublisher {

    private static final Logger logger = LoggerFactory.getLogger(StatusPublisher.class);

    private final ProducerTemplate producerTemplate;

    public StatusPublisher(ProducerTemplate producerTemplate) {
        this.producerTemplate = producerTemplate;
    }

    /**
     * Publishes the payment status change message to the designated route for processing..
     *
     * @param statusMessage the payment status message containing the ID, status, and other related information
     *                      to be published.
     * @return a {@code Mono<Void>} that completes when the status change has been published or errors if an exception occurs.
     */
    public Mono<Void> publishStatusChange(PaymentStatusMessage statusMessage) {
        return Mono.fromRunnable(() -> {
            try {
                // Send to direct:publish-status route which handles marshalling and Kafka publishing
                producerTemplate.sendBody("direct:publish-status", statusMessage);
                
                logger.info("Published status change: payment={}, status={}", 
                        statusMessage.id(), statusMessage.status());
            } catch (Exception e) {
                logger.error("Error publishing status change: {}", e.getMessage(), e);
                throw new RuntimeException("Failed to publish status change", e);
            }
        });
    }
}
