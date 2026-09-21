package dev.philippelachaud.payment.router;

import dev.philippelachaud.payment.handler.PaymentHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.server.RequestPredicates;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerResponse;

/**
 * REST API Router configuration for Payment Service.
 * 
 * <p>Defines the routing for payment-related HTTP endpoints.
 * All endpoints are accessible through Traefik reverse proxy at <code>/api</code> prefix.
 * 
 * <h2>Available Endpoints:</h2>
 * 
 * <h3>1. Create Payment</h3>
 * <ul>
 *   <li><b>URL:</b> <code>PUT /payment/create</code></li>
 *   <li><b>Via Traefik:</b> <code>PUT http://localhost:8000/api/payment/create</code></li>
 *   <li><b>Content-Type:</b> <code>application/json</code></li>
 *   <li><b>Description:</b> Creates a new payment by publishing a CREATE message to Kafka</li>
 *   <li><b>Request Body:</b>
 *     <pre>{
 *   "fromAccount": "CH9300762011623852957",
 *   "toAccount": "CH1234567890123456789",
 *   "amount": 100.00,
 *   "currency": "CHF",
 *   "reference": "Payment reference",
 *   "status": "PENDING",
 *   "createdAt": "2026-09-21T10:00:00Z"
 * }</pre>
 *   </li>
 *   <li><b>Success Response (201 Created):</b>
 *     <pre>{ "id": "550e8400-e29b-41d4-a716-446655440000" }</pre>
 *   </li>
 *   <li><b>Error Response (400 Bad Request):</b>
 *     <pre>{ "error": "Error message" }</pre>
 *   </li>
 * </ul>
 * 
 * <h3>2. Cancel Payment</h3>
 * <ul>
 *   <li><b>URL:</b> <code>DELETE /payment/cancel?id={paymentId}</code></li>
 *   <li><b>Via Traefik:</b> <code>DELETE http://localhost:8000/api/payment/cancel?id={paymentId}</code></li>
 *   <li><b>Description:</b> Cancels an existing payment by publishing a CANCEL message to Kafka</li>
 *   <li><b>Query Parameters:</b>
 *     <ul>
 *       <li><code>id</code> (required) - The UUID of the payment to cancel</li>
 *     </ul>
 *   </li>
 *   <li><b>Success Response (200 OK):</b> Empty body</li>
 *   <li><b>Error Response (400 Bad Request):</b> Error message string</li>
 * </ul>
 * 
 * <h3>3. Payment Status (WebSocket)</h3>
 * <ul>
 *   <li><b>URL:</b> <code>ws://localhost:8000/api/payment/status</code></li>
 *   <li><b>Protocol:</b> WebSocket</li>
 *   <li><b>Description:</b> Real-time payment status updates for all payments</li>
 *   <li><b>Note:</b> This endpoint is configured in WebSocketConfig, not in this router</li>
 *   <li><b>Message Format:</b>
 *     <pre>{
 *   "id": "550e8400-e29b-41d4-a716-446655440000",
 *   "status": "CREATED",
 *   "updatedAt": "2026-09-21T10:00:00Z",
 *   "message": "Payment created successfully"
 * }</pre>
 *   </li>
 * </ul>
 * 
 * <h2>Architecture:</h2>
 * <p>This service uses event-driven architecture:</p>
 * <ol>
 *   <li>HTTP endpoints publish messages to Kafka topics (payment-topic)</li>
 *   <li>Processor service consumes from payment-topic and processes payments</li>
 *   <li>Processor publishes status updates to payment-status topic</li>
 *   <li>Payment service consumes from payment-status and broadcasts via WebSocket</li>
 * </ol>
 * 
 * @see dev.philippelachaud.payment.handler.PaymentHandler
 * @see dev.philippelachaud.payment.config.WebSocketConfig
 */
@Configuration
public class PaymentRouter {
    final PaymentHandler paymentHandler;

    public PaymentRouter(PaymentHandler paymentHandler) {
        this.paymentHandler = paymentHandler;
    }

    @Bean
    public RouterFunction<ServerResponse> paymentRoutes() {
        return RouterFunctions.route()
                .PUT("/payment/create", RequestPredicates.accept(MediaType.APPLICATION_JSON),
                        paymentHandler::createPayment)
                .DELETE("/payment/cancel", paymentHandler::cancelPayment)
                .build();
    }
}
