.PHONY: all build run run-master run-workers clean kill logs output help

# Default target
all: build

# Build both binaries
build:
	cargo build

# Run master in background (logs to master.log)
run-master: build
	@echo "Starting Master in background..."
	@./target/debug/master > master.log 2>&1 &
	@sleep 1
	@echo "Master running on port 7799 (logs: master.log)"
	@lsof -i :7799 2>/dev/null | head -2

# Run 3 workers in background
run-workers: run-master
	@echo "Starting 3 workers in background..."
	@./target/debug/worker > worker1.log 2>&1 &
	@./target/debug/worker > worker2.log 2>&1 &
	@./target/debug/worker > worker3.log 2>&1 &
	@sleep 1
	@echo "3 workers started (logs: worker1.log, worker2.log, worker3.log)"

# Run full distributed MapReduce (master + 3 workers)
# Workers run in foreground so you can see output
run: build
	@echo "Starting Master in background..."
	@./target/debug/master > master.log 2>&1 &
	@sleep 2
	@echo "Starting 3 workers..."
	@echo "======================================"
	@./target/debug/worker
	@echo "======================================"

# Run with specified number of workers (e.g., make run-n N=5)
run-n: build
	@echo "Starting Master in background..."
	@./target/debug/master > master.log 2>&1 &
	@sleep 2
	@echo "Starting $(N) workers..."
	@for i in $$(seq 1 $(N)); do \
		./target/debug/worker > worker$$i.log 2>&1 & \
	done

# Clean build artifacts and logs
clean:
	rm -rf output master.log worker*.log

# Kill all running processes
kill:
	@pkill -9 -f "target/debug/master" 2>/dev/null || true
	@pkill -9 -f "target/debug/worker" 2>/dev/null || true
	@lsof -ti :7799 | xargs -r kill -9 2>/dev/null || true

# Full rebuild
rebuild: clean build

# Show logs
logs:
	@echo "=== Master Log ==="
	@cat master.log 2>/dev/null || echo "No master.log"
	@echo ""
	@echo "=== Worker 1 Log ==="
	@cat worker1.log 2>/dev/null || echo "No worker1.log"

# Show output files
output:
	@echo "=== Final Output Files ==="
	@ls -la output/mr-out-* 2>/dev/null || echo "No output files"
	@echo ""
	@echo "=== Output Content ==="
	@cat output/mr-out-* 2>/dev/null || echo "No output"

# Show help
help:
	@echo "MapReduce Distributed Makefile"
	@echo ""
	@echo "Commands:"
	@echo "  make build        - Build the project"
	@echo "  make run         - Run master + 3 workers (workers in foreground)"
	@echo "  make run-master   - Run only master in background"
	@echo "  make run-workers  - Run master + 3 workers (all in background)"
	@echo "  make run-n N=5   - Run master + N workers"
	@echo "  make clean       - Clean output and log files"
	@echo "  make kill        - Kill all running processes"
	@echo "  make logs        - Show master and worker logs"
	@echo "  make output      - Show final output files"
	@echo "  make rebuild     - Clean and rebuild"
	@echo "  make help        - Show this help"
