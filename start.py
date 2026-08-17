"""Process supervisor for the Packet-CRM ecosystem.

Spawns the API and the Kafka consumer, then holds both open. If either exits,
the other is terminated rather than left running half a system: a consumer
with no API forwards every packet into a connection error, and an API with no
consumer silently stops receiving work (F12).
"""
import signal
import subprocess
import sys
import time

# How long a child gets to shut down gracefully before SIGKILL. Must exceed
# the consumer's SHUTDOWN_DRAIN_SECONDS so its drain can actually finish.
TERMINATE_GRACE_SECONDS = 30

_children = []
_stopping = False


def _terminate_all():
    """SIGTERM every child, then SIGKILL whatever ignored it."""
    global _stopping
    if _stopping:
        return
    _stopping = True

    for name, process in _children:
        if process.poll() is None:
            print(f"Stopping {name}.")
            process.terminate()

    deadline = time.monotonic() + TERMINATE_GRACE_SECONDS
    for name, process in _children:
        remaining = max(0.0, deadline - time.monotonic())
        try:
            process.wait(timeout=remaining)
        except subprocess.TimeoutExpired:
            print(f"{name} did not stop within {TERMINATE_GRACE_SECONDS}s; killing.")
            process.kill()
            process.wait()


def _handle_signal(signum, _frame):
    print(f"\nReceived signal {signum}; stopping services.")
    _terminate_all()
    sys.exit(0)


def main():
    print("Starting the Packet-CRM ecosystem.\n")

    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, _handle_signal)

    print("Starting the API server (main_api.py).")
    _children.append(("API", subprocess.Popen([sys.executable, "src/main_api.py"])))

    # Give the API a moment to bind its port before the consumer starts
    # forwarding to it.
    time.sleep(2)

    print("Starting the Kafka consumer (main_consumer.py).")
    _children.append(("Consumer", subprocess.Popen([sys.executable, "src/main_consumer.py"])))

    print("\nBoth services are running. Press Ctrl+C to stop them.")

    try:
        # Wait on BOTH concurrently. The previous version waited on the API
        # and only then on the consumer, so an API crash left the consumer
        # running unattended while the supervisor sat blocked.
        while True:
            for name, process in _children:
                code = process.poll()
                if code is not None:
                    print(f"\n{name} exited with code {code}; stopping the other service.")
                    _terminate_all()
                    sys.exit(code or 0)
            time.sleep(0.5)
    except KeyboardInterrupt:
        _handle_signal(signal.SIGINT, None)


if __name__ == "__main__":
    main()
