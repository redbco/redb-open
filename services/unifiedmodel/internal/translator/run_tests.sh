#!/bin/bash

# Translator V2 Test Runner Script
# This script runs the complete test suite for the Translator V2 implementation

set -e

echo "🧪 Translator V2 Test Suite Runner"
echo "=================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if we're in the right directory
if [[ ! -f "core/translator.go" ]]; then
    print_error "Please run this script from the translator directory"
    exit 1
fi

# Parse command line arguments
RUN_BENCHMARKS=false
RUN_COVERAGE=false
RUN_RACE=false
VERBOSE=false
SPECIFIC_TEST=""

while [[ $# -gt 0 ]]; do
    case $1 in
        -b|--benchmarks)
            RUN_BENCHMARKS=true
            shift
            ;;
        -c|--coverage)
            RUN_COVERAGE=true
            shift
            ;;
        -r|--race)
            RUN_RACE=true
            shift
            ;;
        -v|--verbose)
            VERBOSE=true
            shift
            ;;
        -t|--test)
            SPECIFIC_TEST="$2"
            shift 2
            ;;
        -h|--help)
            echo "Usage: $0 [OPTIONS]"
            echo "Options:"
            echo "  -b, --benchmarks    Run benchmark tests"
            echo "  -c, --coverage      Generate coverage report"
            echo "  -r, --race          Run with race detection"
            echo "  -v, --verbose       Verbose output"
            echo "  -t, --test NAME     Run specific test"
            echo "  -h, --help          Show this help"
            exit 0
            ;;
        *)
            print_error "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Build test flags
TEST_FLAGS=""
if [[ "$VERBOSE" == "true" ]]; then
    TEST_FLAGS="$TEST_FLAGS -v"
fi
if [[ "$RUN_RACE" == "true" ]]; then
    TEST_FLAGS="$TEST_FLAGS -race"
fi
if [[ -n "$SPECIFIC_TEST" ]]; then
    TEST_FLAGS="$TEST_FLAGS -run $SPECIFIC_TEST"
fi

print_status "Starting Translator V2 test suite..."

# Function to run tests for a specific package
run_package_tests() {
    local package=$1
    local description=$2
    
    print_status "Running $description..."
    
    if go test $TEST_FLAGS ./$package 2>&1; then
        print_success "$description passed"
        return 0
    else
        print_error "$description failed"
        return 1
    fi
}

# Track test results
FAILED_TESTS=()
TOTAL_TESTS=0

# Run tests for each package
PACKAGES=(
    "core:Core translator tests"
    "same_paradigm:Same-paradigm translation tests"
    "cross_paradigm:Cross-paradigm translation tests"
    "utils:Validation and utility tests"
)

for package_info in "${PACKAGES[@]}"; do
    IFS=':' read -r package description <<< "$package_info"
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    
    if ! run_package_tests "$package" "$description"; then
        FAILED_TESTS+=("$description")
    fi
done

# Run integration tests
print_status "Running integration tests..."
TOTAL_TESTS=$((TOTAL_TESTS + 1))
if go test $TEST_FLAGS -run TestTranslatorV2_EndToEnd . 2>&1; then
    print_success "Integration tests passed"
else
    print_error "Integration tests failed"
    FAILED_TESTS+=("Integration tests")
fi

# Run all tests together
if [[ -z "$SPECIFIC_TEST" ]]; then
    print_status "Running complete test suite..."
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    if go test $TEST_FLAGS ./... 2>&1; then
        print_success "Complete test suite passed"
    else
        print_error "Complete test suite failed"
        FAILED_TESTS+=("Complete test suite")
    fi
fi

# Run benchmarks if requested
if [[ "$RUN_BENCHMARKS" == "true" ]]; then
    print_status "Running benchmark tests..."
    
    echo ""
    echo "📊 Performance Benchmarks"
    echo "========================"
    
    # Run different benchmark categories
    print_status "Same-paradigm benchmarks..."
    go test -bench=BenchmarkTranslatorV2_SameParadigm -benchmem .
    
    print_status "Cross-paradigm benchmarks..."
    go test -bench=BenchmarkTranslatorV2_CrossParadigm -benchmem .
    
    print_status "Enrichment analyzer benchmarks..."
    go test -bench=BenchmarkEnrichmentAnalyzer -benchmem .
    
    print_status "Memory allocation benchmarks..."
    go test -bench=BenchmarkTranslatorV2_MemoryAllocation -benchmem .
    
    print_status "Concurrent benchmarks..."
    go test -bench=BenchmarkTranslatorV2_Concurrent -cpu=1,2,4 .
    
    print_success "Benchmark tests completed"
fi

# Generate coverage report if requested
if [[ "$RUN_COVERAGE" == "true" ]]; then
    print_status "Generating coverage report..."
    
    # Run tests with coverage
    go test ./... -coverprofile=coverage.out -covermode=atomic
    
    # Generate HTML report
    go tool cover -html=coverage.out -o coverage.html
    
    # Show coverage summary
    echo ""
    echo "📈 Coverage Summary"
    echo "=================="
    go tool cover -func=coverage.out | tail -1
    
    # Show detailed coverage
    echo ""
    echo "📋 Detailed Coverage"
    echo "==================="
    go tool cover -func=coverage.out
    
    print_success "Coverage report generated: coverage.html"
fi

# Print summary
echo ""
echo "📋 Test Summary"
echo "==============="

if [[ ${#FAILED_TESTS[@]} -eq 0 ]]; then
    print_success "All tests passed! ✅"
    echo "Total test packages: $TOTAL_TESTS"
    echo "Failed: 0"
    echo "Success rate: 100%"
else
    print_error "Some tests failed! ❌"
    echo "Total test packages: $TOTAL_TESTS"
    echo "Failed: ${#FAILED_TESTS[@]}"
    echo "Success rate: $(( (TOTAL_TESTS - ${#FAILED_TESTS[@]}) * 100 / TOTAL_TESTS ))%"
    echo ""
    echo "Failed tests:"
    for test in "${FAILED_TESTS[@]}"; do
        echo "  - $test"
    done
fi

# Additional information
echo ""
echo "📚 Additional Commands"
echo "====================="
echo "Run specific test:     go test -v -run TestSpecificFunction"
echo "Run with race detection: go test -race ./..."
echo "Run benchmarks:        go test -bench=. -benchmem"
echo "Generate coverage:     go test -coverprofile=coverage.out ./..."
echo "View coverage:         go tool cover -html=coverage.out"
echo ""
echo "For more information, see TEST_README.md"

# Exit with appropriate code
if [[ ${#FAILED_TESTS[@]} -eq 0 ]]; then
    exit 0
else
    exit 1
fi
