#!/bin/bash

COUNT=${1:-10}

for i in $(seq 1 $COUNT); do
    curl -s -X POST http://localhost:8080/jobs \
        -H "Content-Type: application/json" \
        -d "{\"queue\": \"default\", \"payload\": {\"task\": \"job-$i\"}}"
    echo
done

echo "seeded $COUNT jobs"
