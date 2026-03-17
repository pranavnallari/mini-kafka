#!/bin/bash
curl -X POST http://localhost:8080/topics/payments/messages/retry \
  -H "Content-Type: application/json" \
  -d '{"group_name": "billing", "partition_index": 0, "offset": 0, "payload": "hello from producer"}'