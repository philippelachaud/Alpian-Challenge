package dev.philippelachaud.processor.route;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.philippelachaud.processor.dto.PaymentStatusMessage;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.jackson.JacksonDataFormat;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class StatusPublisherRoute extends RouteBuilder {

    private final ObjectMapper objectMapper;

    @Value("${kafka.bootstrap-servers:localhost:9092}")
    private String kafkaBootstrapServers;

    @Value("${kafka.topic.payment-status:payment-status}")
    private String paymentStatusTopic;

    public StatusPublisherRoute(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public void configure() throws Exception {
        JacksonDataFormat jsonFormat = new JacksonDataFormat(PaymentStatusMessage.class);
        jsonFormat.setObjectMapper(objectMapper);

        from("direct:publish-status")
                .routeId("status-publisher-route")
                .log("Publishing status to payment-status topic: payment=${body.id}, status=${body.status}")
                .setHeader("kafka.KEY", simple("${body.id}"))
                .marshal(jsonFormat)
                .to("kafka:" + paymentStatusTopic +
                        "?brokers=" + kafkaBootstrapServers +
                        "&keySerializer=org.apache.kafka.common.serialization.StringSerializer" +
                        "&valueSerializer=org.apache.kafka.common.serialization.StringSerializer")
                .log("Status published successfully");
    }
}
