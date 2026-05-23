#!/bin/bash

COUNT=${1:-10}

docker exec -i pg-aqueduct psql -U postgres -d aqueduct -c "DELETE FROM jobs;" > /dev/null

for i in $(seq 1 $COUNT); do
    curl -s -X POST http://localhost:8080/jobs \
        -H "Content-Type: application/json" \
        -d "{\"queue\": \"default\", \"payload\": {\"task\": \"job-$i\"}}"
    echo
done

echo "seeded $COUNT jobs"
