.PHONY: build run run-master run-worker clean kill

# Default target - build first
all: build

# Build both binaries
build:
	cargo build

# Run master (in background, logs to master.log)
run-master: build
	@echo "Starting Master in background..."
	@echo "(Logs will be saved to master.log)"
	@./target/debug/master > master.log 2>&1 &
	@sleep 1
	@echo "Master is running on port 8080"

# Run worker (in foreground to see output)
run-worker: run-master
	@echo "Starting Worker (you will see worker output below)..."
	@./target/debug/worker

# Run both - master in background, worker in foreground
# This way you see worker output in terminal, master logs to file
run: build
	@echo "Starting Master in background (logs -> master.log)..."
	@./target/debug/master > master.log 2>&1 &
	@sleep 2
	@echo "Starting Worker (foreground)..."
	@echo "=========================================="
	@./target/debug/worker

# Clean build artifacts
clean:
	cargo clean
	rm -rf output master.log

# Kill processes
kill:
	@pkill -9 -f "target/debug/master" 2>/dev/null || true
	@pkill -9 -f "target/debug/worker" 2>/dev/null || true
	@lsof -ti :8080 | xargs -r kill -9 2>/dev/null || true

# Full rebuild
rebuild: clean build
