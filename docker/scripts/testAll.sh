#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

MAX_ATTEMPTS=30
TIMEOUT=10

# First argument is the script to run
# Second argument is the expected number of events
# Third argument is the expected number of pages
runTest () {
    echo -n "Running test: $1 - "

    QUERY_RESPONSE="$(${SCRIPT_DIR}/$1)"

    if [[ "$QUERY_RESPONSE" == *"Returned $2 events"* ]] ; then
        if [ ! -z "$3" ] ; then
            if [[ "$QUERY_RESPONSE" == *"Returned $3 pages"* ]] ; then
                echo "SUCCESS: Returned $2 events and $3 pages"
            else
                echo "FAILED: Unexpected number of pages returned"
                echo
                echo "TEST RESPONSE"
                echo "$QUERY_RESPONSE"
                exit 1
            fi
        else
            echo "SUCCESS: Returned $2 events"
        fi
    else
        echo "FAILURE: Unexpected number of events returned"
        echo
        echo "TEST RESPONSE"
        echo "$QUERY_RESPONSE"
        exit 1
    fi
}

echo "Waiting for services to be ready..."

attempt=0
while [ $attempt -lt $MAX_ATTEMPTS ]; do
    echo "Checking query and executor status (${attempt}/${MAX_ATTEMPTS})"

    QUERY_STATUS=$(curl -s -m 5 http://localhost:8080/query/mgmt/health | grep UP)
    EXEC_STATUS=$(curl -s -m 5 http://localhost:8380/executor/mgmt/health | grep UP)
    if [ "${QUERY_STATUS}" == "{\"status\":\"UP\"}" ] && [ "${EXEC_STATUS}" == "{\"status\":\"UP\"}" ] ; then
        echo "Query and Executor Services ready"
        break
    fi

    sleep ${TIMEOUT}

    ((attempt++))
done

if [ $attempt == $MAX_ATTEMPTS ]; then
    echo "FAILURE! Query and/or Executor Services never became ready"
    exit 1
fi

echo "Running tests..."

echo

runTest batchLookup.sh 2
runTest batchLookupContent.sh 4
runTest count.sh 12 2
runTest discovery.sh 2 1
# runTest edge.sh 0 0
# runTest edgeEvent.sh 1 1
runTest errorCount.sh 1 1
runTest errorDiscovery.sh 1 1
runTest errorFieldIndexCount.sh 1 1
runTest errorQuery.sh 1 1
runTest fieldIndexCount.sh 12 2
runTest hitHighlights.sh 12 2
runTest lookup.sh 1
runTest lookupContent.sh 2
# runTest metrics.sh 0 0
runTest query.sh 12 2

$SCRIPT_DIR/cleanup.sh

echo
echo "All tests SUCCEEDED!"
