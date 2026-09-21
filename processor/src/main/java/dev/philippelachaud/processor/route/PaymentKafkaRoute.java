package dev.philippelachaud.processor.route;

import dev.philippelachaud.processor.dto.PaymentMessage;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.jackson.JacksonDataFormat;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class PaymentKafkaRoute extends RouteBuilder {

    @Value("${kafka.bootstrap-servers:localhost:9092}")
    private String kafkaBootstrapServers;

    @Value("${kafka.consumer.group-id:payment-processor-group}")
    private String consumerGroupId;

    @Value("${kafka.topic.payment:payment-topic}")
    private String paymentTopic;

    private final PaymentProcessor paymentProcessor;

    public PaymentKafkaRoute(PaymentProcessor paymentProcessor) {
        this.paymentProcessor = paymentProcessor;
    }

    @Override
    public void configure() throws Exception {

        JacksonDataFormat jsonFormat = new JacksonDataFormat(PaymentMessage.class);

        onException(Exception.class)
                .handled(true)
                .log("Error processing payment: ${exception.message}")
                .to("log:payment-error?level=ERROR");

        // Consume from payment-topic and process payments
        from("kafka:" + paymentTopic +
                "?brokers=" + kafkaBootstrapServers +
                "&groupId=" + consumerGroupId +
                "&autoOffsetReset=earliest" +
                "&keyDeserializer=org.apache.kafka.common.serialization.StringDeserializer" +
                "&valueDeserializer=org.apache.kafka.common.serialization.StringDeserializer")
                .routeId("payment-consumer-route")
                .log("Received message from payment-topic: ${body}")
                .unmarshal(jsonFormat)
                .process(paymentProcessor)
                .log("Payment processed successfully");
    }
}
