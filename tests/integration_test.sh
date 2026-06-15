#!/bin/bash
echo "DataGuardian: Integration Test"
echo

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Test counter
PASSED=0
FAILED=0

test_endpoint() {
    local method=$1
    local endpoint=$2
    local expected_code=$3
    
    echo -n "Testing $method $endpoint ... "
    
    response=$(curl -s -w "\n%{http_code}" -X $method "http://localhost:8080/api/v1${endpoint}")
    http_code=$(echo "$response" | tail -n1)
    body=$(echo "$response" | sed '$d')
    
    if [ "$http_code" == "$expected_code" ]; then
        echo -e "${GREEN}PASS${NC} ($http_code)"
        PASSED=$((PASSED + 1))
        echo "$body" | jq . 2>/dev/null || echo "$body"
    else
        echo -e "${RED}FAIL${NC} (expected $expected_code, got $http_code)"
        FAILED=$((FAILED + 1))
    fi
    
    echo
}

echo "Health Checks"

test_endpoint GET "/health" 200
test_endpoint GET "/info" 200
test_endpoint GET "/status" 200
echo

echo "Metrics Endpoints"

test_endpoint GET "/metrics" 200
test_endpoint GET "/metrics/Hospital_A" 200
test_endpoint GET "/metrics/Hospital_B" 200
test_endpoint GET "/metrics/Hospital_C" 200
echo


echo "Test Results"
echo -e "${GREEN}Passed: $PASSED${NC}"
echo -e "${RED}Failed: $FAILED${NC}"


exit $FAILED