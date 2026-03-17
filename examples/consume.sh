#!/bin/bash
# "order-123" hashes to partition 1 with FNV-32a mod 3
curl -X GET "http://localhost:8080/topics/payments/messages?group=billing&partition=1&limit=10"