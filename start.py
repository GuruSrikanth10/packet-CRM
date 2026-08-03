import subprocess
import sys
import time

def main():
    print("Starting Packet-CRM Ecosystem...\n")
    
    # Start the API Server
    print("Starting API Server (main_api.py)...")
    api_process = subprocess.Popen([sys.executable, "src/main_api.py"])
    
    # Give the API a second to spin up
    time.sleep(2)
    
    # Start the Kafka Consumer
    print("Starting Kafka Consumer (main_consumer.py)...")
    consumer_process = subprocess.Popen([sys.executable, "src/main_consumer.py"])
    
    print("\nBoth services are running! Press Ctrl+C to stop them.")
    
    try:
        # Wait indefinitely while both run
        api_process.wait()
        consumer_process.wait()
    except KeyboardInterrupt:
        print("\nStopping services...")
        api_process.terminate()
        consumer_process.terminate()
        api_process.wait()
        consumer_process.wait()
        print("Goodbye!")

if __name__ == "__main__":
    main()
