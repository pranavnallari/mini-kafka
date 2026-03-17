#!/bin/bash
curl -X POST http://localhost:8080/topics/payments/messages \
  -H "Content-Type: application/json" \
  -d '{"key": "order-123", "payload": "hello from producer"}'