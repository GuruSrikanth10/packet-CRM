import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.utils.kafkaConsumer import consume_forever

if __name__ == "__main__":
    import threading
    # Can run in main thread since it's just the consumer
    consume_forever()
