#!/bin/bash

waitForGarage() {
    garageCount="0"
    while [ "$garageCount" -ne 1 ]; do
        sleep 1

        (curl -s -v -H "Authorization: Bearer test_admin_token" http://localhost:3903/health >/dev/null 2> /tmp/curl_garage_health.out) || true
        garageCount=$(grep -c "$1" /tmp/curl_garage_health.out)
    done
}

garage server > /var/log/garage.log 2>&1 &
waitForGarage "503 Service Unavailable"

NODE_ID=$(garage status | awk 'NR==3 {print $1}')

garage layout assign "$NODE_ID" --zone local --capacity 100G
garage layout apply --version 1

waitForGarage "200 OK"

garage json-api ImportKey '{"name":"test-key","accessKeyId":"GK123456789012345678901234","secretAccessKey":"1234567890123456789012345678901234567890123456789012345678901234","spec":{"read":{"all":true},"write":{"all":true},"admin":{"all":true},"createBuckets":true}}'
garage key allow --create-bucket GK123456789012345678901234

sleep infinity
