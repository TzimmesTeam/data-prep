#!/bin/sh
set -eu

awslocal sqs create-queue --queue-name data-prep-raw-events-dlq >/dev/null
DLQ_ARN=$(awslocal sqs get-queue-attributes \
  --queue-url http://localhost:4566/000000000000/data-prep-raw-events-dlq \
  --attribute-names QueueArn \
  --query 'Attributes.QueueArn' \
  --output text)
awslocal sqs create-queue \
  --queue-name data-prep-raw-events \
  --attributes "{\"RedrivePolicy\":\"{\\\"deadLetterTargetArn\\\":\\\"${DLQ_ARN}\\\",\\\"maxReceiveCount\\\":\\\"5\\\"}\",\"VisibilityTimeout\":\"300\"}" >/dev/null
