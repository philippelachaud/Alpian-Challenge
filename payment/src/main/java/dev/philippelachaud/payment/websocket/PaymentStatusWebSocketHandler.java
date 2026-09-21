package dev.philippelachaud.payment.websocket;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.philippelachaud.payment.dto.PaymentStatusMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

@Component
public class PaymentStatusWebSocketHandler implements WebSocketHandler {
    private static final Logger logger = LoggerFactory.getLogger(PaymentStatusWebSocketHandler.class);
    
    private final ObjectMapper objectMapper;
    private final Sinks.Many<PaymentStatusMessage> broadcastSink;
    
    public PaymentStatusWebSocketHandler(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        // Create a multicast sink that can have multiple subscribers
        this.broadcastSink = Sinks.many().multicast().onBackpressureBuffer();
    }

    /***
     * Handles WebSocket connections and broadcasts payment status updates to connected clients.
     *
     * @param session The WebSocket session.
     * @return A Mono that completes when the WebSocket connection is closed.
     */
    @Override
    public Mono<Void> handle(WebSocketSession session) {
        String sessionId = session.getId();
        logger.info("WebSocket connection established - session: {}", sessionId);
        
        // Create a flux that emits all payment status updates to the WebSocket clients
        Flux<String> messageFlux = broadcastSink.asFlux()
                .map(this::toJson)
                .doOnNext(json -> logger.debug("Sending status update to session {}: {}", sessionId, json))
                .doOnSubscribe(subscription -> 
                    logger.info("Client subscribed to payment status updates - session: {}", sessionId))
                .doFinally(signalType -> 
                    logger.info("WebSocket connection closed - session: {} (signal: {})", 
                            sessionId, signalType));

        return session.send(messageFlux.map(session::textMessage));
    }

    /**
     * Broadcasts a payment status update to all connected WebSocket clients.
     *
     * @param statusMessage The payment status message to be broadcasted, containing the payment ID and status.
     */
    public void broadcastStatus(PaymentStatusMessage statusMessage) {
        logger.info("Broadcasting status update to all clients: payment={}, status={}", 
                statusMessage.id(), statusMessage.status());
        
        Sinks.EmitResult result = broadcastSink.tryEmitNext(statusMessage);
        if (result.isFailure()) {
            logger.warn("Failed to emit status update: payment={}, result={}", 
                    statusMessage.id(), result);
        } else {
            logger.debug("Status update broadcast successful: payment={}", statusMessage.id());
        }
    }

    private String toJson(PaymentStatusMessage message) {
        try {
            return objectMapper.writeValueAsString(message);
        } catch (Exception e) {
            logger.error("Error converting status message to JSON", e);
            return "{\"error\":\"Failed to serialize message\"}";
        }
    }
}
