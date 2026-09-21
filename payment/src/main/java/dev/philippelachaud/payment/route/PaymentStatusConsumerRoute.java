package dev.philippelachaud.payment.route;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.philippelachaud.payment.dto.PaymentStatusMessage;
import dev.philippelachaud.payment.websocket.PaymentStatusWebSocketHandler;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.jackson.JacksonDataFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class PaymentStatusConsumerRoute extends RouteBuilder {
    private static final Logger logger = LoggerFactory.getLogger(PaymentStatusConsumerRoute.class);
    
    private final PaymentStatusWebSocketHandler webSocketHandler;
    private final ObjectMapper objectMapper;
    
    @Value("${payment.status.topic:payment-status}")
    private String paymentStatusTopic;
    
    @Value("${kafka.brokers}")
    private String kafkaBootstrapServers;
    
    @Value("${payment.status.consumer.group:payment-service-status-consumer}")
    private String consumerGroupId;
    
    public PaymentStatusConsumerRoute(
            PaymentStatusWebSocketHandler webSocketHandler,
            ObjectMapper objectMapper) {
        this.webSocketHandler = webSocketHandler;
        this.objectMapper = objectMapper;
    }
    
    @Override
    public void configure() throws Exception {
        // Use to unmarshal PaymentStatusMessage, when receiving messages from the payment-status topic
        JacksonDataFormat jsonFormat = new JacksonDataFormat(PaymentStatusMessage.class);
        jsonFormat.setObjectMapper(objectMapper);
        
        // Kafka consumer route for payment-status topic
        // When received, the message is unmarshalled into a PaymentStatusMessage object
        // and published to the payment-status-websocket topic
        from("kafka:" + paymentStatusTopic +
                "?brokers=" + kafkaBootstrapServers +
                "&groupId=" + consumerGroupId +
                "&autoOffsetReset=earliest" +
                "&keyDeserializer=org.apache.kafka.common.serialization.StringDeserializer" +
                "&valueDeserializer=org.apache.kafka.common.serialization.StringDeserializer")
                .routeId("payment-status-consumer-route")
                .log("Received message from payment-status topic: ${body}")
                .unmarshal(jsonFormat)
                .process(exchange -> {
                    // Unmarshal the message into a PaymentStatusMessage object
                    PaymentStatusMessage statusMessage = exchange.getIn().getBody(PaymentStatusMessage.class);
                    logger.info("Processing status message: payment={}, status={}", 
                            statusMessage.id(), statusMessage.status());
                    
                    // Broadcast the status message to any WebSocket clients
                    webSocketHandler.broadcastStatus(statusMessage);
                    
                    logger.info("Status message broadcast to WebSocket clients");
                })
                .log("Payment status message processed successfully");
    }
}
