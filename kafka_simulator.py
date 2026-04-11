"""
Real-Time MoMo Transaction Stream Simulator
=============================================
Simulates Apache Kafka-style real-time streaming of Ghana
Mobile Money transactions using Python threading.

In a production environment this would use:
    - Apache Kafka as the message broker
    - kafka-python or confluent-kafka as the client library
    - Multiple consumer groups processing different event types

This simulator replicates the same architecture using:
    - Python Queue as the message broker (thread-safe)
    - Producer thread generating live MoMo transactions
    - Multiple consumer threads processing different event types
    - Real-time metrics printed to console every 5 seconds

Kafka Concepts Demonstrated:
    Producer  → generates and publishes transaction events
    Topic     → transaction stream (the Queue)
    Consumers → fraud detector, metrics aggregator, logger
    Partitions → simulated via multiple consumer threads

Author: Lawrence Koomson
GitHub: github.com/lawrykoomson
"""

import queue
import threading
import time
import random
import json
import logging
from datetime import datetime
from pathlib import Path
from collections import defaultdict

# ─────────────────────────────────────────────
#  LOGGING
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[
        logging.FileHandler("kafka_simulator.log"),
        logging.StreamHandler()
    ]
)

# ─────────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────────
TOPIC_NAME          = "momo.transactions.live"
PARTITION_COUNT     = 3
PRODUCER_RATE_HZ    = 5        # transactions per second
SIMULATION_SECONDS  = 60       # run for 60 seconds
HIGH_VALUE_THRESHOLD = 500     # GHS
RAPID_WINDOW_SECS   = 10       # seconds between transactions = rapid

OPERATORS = ["MTN", "Vodafone", "AirtelTigo"]
TXN_TYPES = ["P2P", "MBP", "WDR", "DEP", "AIRTIME", "INTL"]
REGIONS   = ["Greater Accra", "Ashanti", "Western", "Eastern", "Northern"]
STATUSES  = ["SUCCESS", "SUCCESS", "SUCCESS", "FAILED", "PENDING"]

REPORTS_PATH = Path("data/reports/")
REPORTS_PATH.mkdir(parents=True, exist_ok=True)


# ─────────────────────────────────────────────
#  MESSAGE BROKER (simulates Kafka Topic)
# ─────────────────────────────────────────────
class KafkaTopic:
    """
    Simulates a Kafka topic with multiple partitions.
    Messages are distributed across partitions using round-robin.
    """
    def __init__(self, name: str, partitions: int = 3):
        self.name       = name
        self.partitions = [queue.Queue() for _ in range(partitions)]
        self.partition_counter = 0
        self.total_produced = 0
        self.lock = threading.Lock()

    def produce(self, message: dict):
        """Publish a message to the next partition (round-robin)."""
        with self.lock:
            partition_id = self.partition_counter % len(self.partitions)
            self.partitions[partition_id].put(message)
            self.partition_counter += 1
            self.total_produced += 1

    def consume(self, partition_id: int, timeout: float = 0.1):
        """Read one message from a specific partition."""
        try:
            return self.partitions[partition_id].get(timeout=timeout)
        except queue.Empty:
            return None

    def total_messages(self):
        return sum(p.qsize() for p in self.partitions)


# ─────────────────────────────────────────────
#  PRODUCER — generates live MoMo transactions
# ─────────────────────────────────────────────
class MoMoTransactionProducer(threading.Thread):
    """
    Kafka Producer — simulates live MoMo transaction events.
    Publishes to the momo.transactions.live topic.
    """
    def __init__(self, topic: KafkaTopic, rate_hz: int, duration_secs: int):
        super().__init__(name="MoMoProducer", daemon=True)
        self.topic        = topic
        self.rate_hz      = rate_hz
        self.duration     = duration_secs
        self.produced     = 0
        self.running      = True
        self.logger       = logging.getLogger("Producer")
        self._txn_counter = 1

    def generate_transaction(self) -> dict:
        """Generate one realistic Ghana MoMo transaction event."""
        amount = round(random.choices(
            [random.uniform(1, 50),
             random.uniform(50, 500),
             random.uniform(500, 5000)],
            weights=[60, 30, 10]
        )[0], 2)

        return {
            "event_type":       "TRANSACTION",
            "topic":            self.topic.name,
            "partition":        self._txn_counter % PARTITION_COUNT,
            "offset":           self._txn_counter,
            "timestamp":        datetime.now().isoformat(),
            "transaction_id":   f"TXN-LIVE-{str(self._txn_counter).zfill(8)}",
            "sender_msisdn":    f"024{random.randint(1000000, 9999999)}",
            "receiver_msisdn":  f"055{random.randint(1000000, 9999999)}",
            "amount_ghs":       amount,
            "fee_ghs":          round(amount * random.uniform(0.005, 0.015), 2),
            "transaction_type": random.choice(TXN_TYPES),
            "operator":         random.choices(
                                    OPERATORS, weights=[55, 25, 20])[0],
            "status":           random.choice(STATUSES),
            "region":           random.choices(
                                    REGIONS,
                                    weights=[35, 25, 15, 15, 10])[0],
            "is_high_value":    amount >= HIGH_VALUE_THRESHOLD,
        }

    def run(self):
        self.logger.info(
            f"Producer started — publishing to topic '{self.topic.name}' "
            f"at {self.rate_hz} msg/sec for {self.duration}s"
        )
        end_time    = time.time() + self.duration
        sleep_time  = 1.0 / self.rate_hz

        while self.running and time.time() < end_time:
            txn = self.generate_transaction()
            self.topic.produce(txn)
            self.produced     += 1
            self._txn_counter += 1
            time.sleep(sleep_time)

        self.running = False
        self.logger.info(
            f"Producer finished — published {self.produced:,} transactions"
        )


# ─────────────────────────────────────────────
#  CONSUMER 1 — Fraud Detector
# ─────────────────────────────────────────────
class FraudDetectorConsumer(threading.Thread):
    """
    Kafka Consumer Group: fraud-detector
    Partition: 0
    Detects high-value and suspicious transactions in real-time.
    """
    def __init__(self, topic: KafkaTopic):
        super().__init__(name="FraudDetector", daemon=True)
        self.topic          = topic
        self.consumed       = 0
        self.alerts         = []
        self.running        = True
        self.logger         = logging.getLogger("FraudDetector")
        self._last_txn_time = {}

    def run(self):
        self.logger.info("Consumer started — monitoring partition 0 for fraud signals")

        while self.running:
            msg = self.topic.consume(partition_id=0)
            if msg is None:
                continue

            self.consumed += 1
            alerts = []

            # Signal 1: High value
            if msg["is_high_value"]:
                alerts.append(f"HIGH_VALUE: GHS {msg['amount_ghs']:,.2f}")

            # Signal 2: Rapid succession
            sender = msg["sender_msisdn"]
            now    = datetime.fromisoformat(msg["timestamp"])
            if sender in self._last_txn_time:
                secs = (now - self._last_txn_time[sender]).total_seconds()
                if secs < RAPID_WINDOW_SECS:
                    alerts.append(f"RAPID_SUCCESSION: {secs:.1f}s since last txn")
            self._last_txn_time[sender] = now

            # Signal 3: Failed high-value
            if msg["status"] == "FAILED" and msg["amount_ghs"] > 200:
                alerts.append(f"FAILED_HIGH_AMOUNT: GHS {msg['amount_ghs']:,.2f}")

            if alerts:
                alert = {
                    "transaction_id": msg["transaction_id"],
                    "amount_ghs":     msg["amount_ghs"],
                    "operator":       msg["operator"],
                    "region":         msg["region"],
                    "alerts":         alerts,
                    "detected_at":    datetime.now().isoformat()
                }
                self.alerts.append(alert)
                self.logger.warning(
                    f"FRAUD ALERT | {msg['transaction_id']} | "
                    f"GHS {msg['amount_ghs']:,.2f} | {' + '.join(alerts)}"
                )


# ─────────────────────────────────────────────
#  CONSUMER 2 — Metrics Aggregator
# ─────────────────────────────────────────────
class MetricsAggregatorConsumer(threading.Thread):
    """
    Kafka Consumer Group: metrics-aggregator
    Partition: 1
    Aggregates real-time KPIs from the transaction stream.
    """
    def __init__(self, topic: KafkaTopic):
        super().__init__(name="MetricsAggregator", daemon=True)
        self.topic    = topic
        self.consumed = 0
        self.running  = True
        self.logger   = logging.getLogger("MetricsAggregator")

        self.metrics = {
            "total_volume_ghs":     0.0,
            "total_fees_ghs":       0.0,
            "total_transactions":   0,
            "successful":           0,
            "failed":               0,
            "high_value_count":     0,
            "by_operator":          defaultdict(float),
            "by_region":            defaultdict(float),
            "by_type":              defaultdict(int),
        }

    def run(self):
        self.logger.info("Consumer started — aggregating metrics from partition 1")

        while self.running:
            msg = self.topic.consume(partition_id=1)
            if msg is None:
                continue

            self.consumed += 1
            m = self.metrics

            m["total_volume_ghs"]   += msg["amount_ghs"]
            m["total_fees_ghs"]     += msg["fee_ghs"]
            m["total_transactions"] += 1
            m["by_operator"][msg["operator"]]        += msg["amount_ghs"]
            m["by_region"][msg["region"]]            += msg["amount_ghs"]
            m["by_type"][msg["transaction_type"]]    += 1

            if msg["status"] == "SUCCESS":
                m["successful"] += 1
            elif msg["status"] == "FAILED":
                m["failed"] += 1
            if msg["is_high_value"]:
                m["high_value_count"] += 1

    def get_snapshot(self) -> dict:
        """Return a copy of current metrics."""
        m = self.metrics
        total = m["total_transactions"]
        return {
            "total_transactions":   total,
            "total_volume_ghs":     round(m["total_volume_ghs"], 2),
            "total_fees_ghs":       round(m["total_fees_ghs"], 2),
            "success_rate_pct":     round(m["successful"] / max(total, 1) * 100, 1),
            "high_value_count":     m["high_value_count"],
            "top_operator":         max(m["by_operator"], key=m["by_operator"].get, default="N/A"),
            "top_region":           max(m["by_region"],   key=m["by_region"].get,   default="N/A"),
        }


# ─────────────────────────────────────────────
#  CONSUMER 3 — Transaction Logger
# ─────────────────────────────────────────────
class TransactionLoggerConsumer(threading.Thread):
    """
    Kafka Consumer Group: transaction-logger
    Partition: 2
    Logs all transactions to a JSONL file for downstream processing.
    """
    def __init__(self, topic: KafkaTopic):
        super().__init__(name="TransactionLogger", daemon=True)
        self.topic    = topic
        self.consumed = 0
        self.running  = True
        self.logger   = logging.getLogger("TransactionLogger")
        self.log_file = REPORTS_PATH / "live_transactions.jsonl"

    def run(self):
        self.logger.info(
            f"Consumer started — logging transactions from partition 2 "
            f"to {self.log_file}"
        )
        with open(self.log_file, "w") as f:
            while self.running:
                msg = self.topic.consume(partition_id=2)
                if msg is None:
                    continue
                self.consumed += 1
                f.write(json.dumps(msg) + "\n")
                f.flush()


# ─────────────────────────────────────────────
#  METRICS DASHBOARD — prints live stats
# ─────────────────────────────────────────────
def print_live_metrics(
    producer:   MoMoTransactionProducer,
    metrics:    MetricsAggregatorConsumer,
    fraud:      FraudDetectorConsumer,
    logger_consumer: TransactionLoggerConsumer,
    interval:   int = 5
):
    """Print real-time pipeline metrics every N seconds."""
    start = time.time()

    while producer.running:
        time.sleep(interval)
        elapsed  = int(time.time() - start)
        snapshot = metrics.get_snapshot()

        print("\n" + "="*65)
        print(f"  KAFKA STREAM — LIVE METRICS  [{elapsed}s elapsed]")
        print("="*65)
        print(f"  Topic            : {TOPIC_NAME}")
        print(f"  Messages Produced: {producer.produced:,}")
        print(f"  Throughput       : {producer.produced / max(elapsed,1):.1f} msg/sec")
        print("-"*65)
        print(f"  Total Transactions : {snapshot['total_transactions']:,}")
        print(f"  Total Volume       : GHS {snapshot['total_volume_ghs']:,.2f}")
        print(f"  Total Fees         : GHS {snapshot['total_fees_ghs']:,.2f}")
        print(f"  Success Rate       : {snapshot['success_rate_pct']}%")
        print(f"  High Value Flagged : {snapshot['high_value_count']:,}")
        print("-"*65)
        print(f"  Top Operator       : {snapshot['top_operator']}")
        print(f"  Top Region         : {snapshot['top_region']}")
        print("-"*65)
        print(f"  FRAUD ALERTS       : {len(fraud.alerts):,} detected")
        print(f"  Transactions Logged: {logger_consumer.consumed:,}")
        print("="*65)


# ─────────────────────────────────────────────
#  MAIN — Run the full streaming simulation
# ─────────────────────────────────────────────
def run_kafka_simulator():
    print("\n" + "="*65)
    print("  GHANA MOMO — KAFKA STREAM SIMULATOR")
    print("  Architecture: Producer → Topic → 3 Consumer Groups")
    print("="*65)
    print(f"  Topic          : {TOPIC_NAME}")
    print(f"  Partitions     : {PARTITION_COUNT}")
    print(f"  Producer Rate  : {PRODUCER_RATE_HZ} transactions/sec")
    print(f"  Duration       : {SIMULATION_SECONDS} seconds")
    print(f"  Total Expected : ~{PRODUCER_RATE_HZ * SIMULATION_SECONDS:,} transactions")
    print("="*65 + "\n")

    # Create topic
    topic = KafkaTopic(TOPIC_NAME, PARTITION_COUNT)

    # Create producer and consumers
    producer         = MoMoTransactionProducer(topic, PRODUCER_RATE_HZ, SIMULATION_SECONDS)
    fraud_detector   = FraudDetectorConsumer(topic)
    metrics_agg      = MetricsAggregatorConsumer(topic)
    txn_logger       = TransactionLoggerConsumer(topic)

    # Start all threads
    for t in [producer, fraud_detector, metrics_agg, txn_logger]:
        t.start()

    # Print live metrics every 10 seconds
    metrics_thread = threading.Thread(
        target=print_live_metrics,
        args=(producer, metrics_agg, fraud_detector, txn_logger, 10),
        daemon=True
    )
    metrics_thread.start()

    # Wait for producer to finish
    producer.join()

    # Allow consumers to drain remaining messages
    time.sleep(3)

    # Stop consumers
    for t in [fraud_detector, metrics_agg, txn_logger]:
        t.running = False

    # Final summary
    final = metrics_agg.get_snapshot()
    print("\n" + "="*65)
    print("  KAFKA SIMULATION — FINAL SUMMARY")
    print("="*65)
    print(f"  Total Produced         : {producer.produced:,} transactions")
    print(f"  Fraud Alerts Generated : {len(fraud_detector.alerts):,}")
    print(f"  Transactions Logged    : {txn_logger.consumed:,}")
    print(f"  Total Volume Streamed  : GHS {final['total_volume_ghs']:,.2f}")
    print(f"  Total Fees Collected   : GHS {final['total_fees_ghs']:,.2f}")
    print(f"  Final Success Rate     : {final['success_rate_pct']}%")
    print(f"  High Value Detected    : {final['high_value_count']:,}")
    print(f"  Top Operator           : {final['top_operator']}")
    print(f"  Top Region             : {final['top_region']}")
    print("-"*65)
    print(f"  Log file saved to      : {txn_logger.log_file}")
    print("="*65 + "\n")

    # Save fraud alerts
    if fraud_detector.alerts:
        import csv
        alerts_path = REPORTS_PATH / "fraud_alerts_live.csv"
        with open(alerts_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fraud_detector.alerts[0].keys())
            writer.writeheader()
            writer.writerows(fraud_detector.alerts)
        print(f"  Fraud alerts saved to  : {alerts_path}")


if __name__ == "__main__":
    run_kafka_simulator()