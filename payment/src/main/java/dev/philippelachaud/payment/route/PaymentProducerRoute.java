package dev.philippelachaud.payment.route;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.philippelachaud.payment.dto.PaymentActionDto;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.jspecify.annotations.NonNull;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Objects;

@Component
public class PaymentProducerRoute extends RouteBuilder {
    final String paymentTopic;
    final String kafkaBrokers;
    final ObjectMapper objectMapper = new ObjectMapper();

    public PaymentProducerRoute(@Value("${payment.topic}") String paymentTopic,
                                @Value("${kafka.brokers}") String kafkaBrokers) {
        this.paymentTopic = paymentTopic;
        this.kafkaBrokers = kafkaBrokers;
    }

    @Override
    public void configure() {
        from("direct:createPayment")
                .routeId("createPayment")
                .process(new Processor() {
                    @Override
                    public void process(@NonNull Exchange exchange) throws Exception {
                        PaymentActionDto<?> actionDto = exchange.getIn().getBody(PaymentActionDto.class);
                        var flatMessage = objectMapper.convertValue(Objects.requireNonNull(actionDto).payload(),
                                Map.class);
                        flatMessage.put("action", actionDto.action().name());
                        String paymentId = (String) flatMessage.get("id");
                        exchange.getIn().setHeader(KafkaConstants.KEY, paymentId);
                        exchange.getIn().setBody(flatMessage);
                    }
                })
                .marshal().json()
                .log("Publishing payment to topic: " + paymentTopic)
                .to("kafka:" + paymentTopic + "?brokers=" + kafkaBrokers);
    }
}
