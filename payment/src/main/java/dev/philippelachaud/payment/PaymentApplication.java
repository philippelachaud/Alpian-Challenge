package dev.philippelachaud.payment;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan({"dev.philippelachaud.payment.service",
        "dev.philippelachaud.payment.router",
        "dev.philippelachaud.payment.handler",
        "dev.philippelachaud.payment.config",
        "dev.philippelachaud.payment.route",
        "dev.philippelachaud.payment.websocket"
})
public class PaymentApplication {

    public static void main(String[] args) {
        SpringApplication.run(PaymentApplication.class, args);
    }

}
