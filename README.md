# Payment Service API Documentation

## Overview

The Payment Service provides REST and WebSocket APIs for managing payments in an event-driven architecture. All endpoints are accessible through Traefik reverse proxy at the `/api` prefix.

**Base URL (via Traefik):** `http://localhost:8000/api`

## How to run it

```shell
$ docker-compose up -d
```

## How to stop it

```shell
$ docker-compose down
```

## Architecture

The service uses event-driven architecture:

1. HTTP endpoints publish messages to Kafka topics (`payment-topic`)
2. Processor service consumes from `payment-topic` and processes payments
3. Processor publishes status updates to `payment-status` topic
4. Payment service consumes from `payment-status` and broadcasts via WebSocket

---

## REST Endpoints

### 1. Create Payment

Creates a new payment by publishing a CREATE message to Kafka.

**Endpoint:** `PUT /payment/create`

**Full URL:** `http://localhost:8000/api/payment/create`

**Headers:**

```
Content-Type: application/json
Accept: application/json
```

**Request Body:**

```json
{
  "fromAccount": "CH9300762011623852957",
  "toAccount": "CH1234567890123456789",
  "amount": 100.0,
  "currency": "CHF",
  "reference": "Payment reference",
  "status": "PENDING",
  "createdAt": "2026-09-21T10:00:00Z"
}
```

**Request Fields:**

- `fromAccount` (string, required) - Source account IBAN
- `toAccount` (string, required) - Destination account IBAN
- `amount` (number, required) - Payment amount
- `currency` (string, required) - Currency code (e.g., CHF, EUR, USD)
- `reference` (string, required) - Payment reference/description
- `status` (string, required) - Initial status (typically "PENDING")
- `createdAt` (string, optional) - ISO 8601 timestamp

**Success Response (201 Created):**

```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000"
}
```

**Error Response (400 Bad Request):**

```json
{
  "error": "Error message describing what went wrong"
}
```

**Example (curl):**

```bash
curl -X PUT http://localhost:8000/api/payment/create \
  -H "Content-Type: application/json" \
  -H "Accept: application/json" \
  -d '{
    "fromAccount": "CH9300762011623852957",
    "toAccount": "CH1234567890123456789",
    "amount": 100.00,
    "currency": "CHF",
    "reference": "Payment for invoice #123",
    "status": "PENDING",
    "createdAt": "2026-09-21T10:00:00Z"
  }'
```

---

### 2. Cancel Payment

Cancels an existing payment by publishing a CANCEL message to Kafka.

**Endpoint:** `DELETE /payment/cancel?id={paymentId}`

**Full URL:** `http://localhost:8000/api/payment/cancel?id={paymentId}`

**Query Parameters:**

- `id` (string, required) - The UUID of the payment to cancel

**Success Response (200 OK):**

- Empty response body
- Status indicates successful cancellation request

**Error Response (400 Bad Request):**

- Plain text error message

**Example (curl):**

```bash
curl -X DELETE "http://localhost:8000/api/payment/cancel?id=550e8400-e29b-41d4-a716-446655440000"
```

---

## WebSocket Endpoint

### Payment Status Updates

Real-time payment status updates for all payments in the system.

**Protocol:** WebSocket

**Description:** Connects to receive real-time status updates for all payments. Once connected, the server will push status messages as they occur.

See `payment-status-monitor.html` in the project root for a HTML/JavaScript example with UI.

---

## Testing

### HTML Monitor

A ready-to-use HTML monitoring page is available: `payment-status-monitor.html`

Open it in your browser to:

- Connect to WebSocket endpoint
- View real-time payment status updates
- See all payment activity in the system

### Complete Workflow Examples

Cf. `src/test/http/payments.http`

## Technical Details

### Technology Stack

- **Framework:** Spring Boot 4.1.1 with WebFlux (reactive)
- **Messaging:** Apache Kafka via Apache Camel
- **WebSocket:** Spring WebFlux WebSocket support
- **Reverse Proxy:** Traefik

### Service Ports

- **Direct Service:** 8080 (internal, not exposed)
- **Via Traefik:** 8000 (public access)
- **Traefik Dashboard:** http://localhost:8080/dashboard/

### Related Services

- **Processor Service:** Handles payment processing logic
- **Kafka:** Message broker for event-driven communication
- **PostgreSQL:** Database for payment persistence (in processor)
