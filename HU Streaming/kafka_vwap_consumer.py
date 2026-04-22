from kafka import KafkaConsumer
from kafka.structs import TopicPartition

BOOTSTRAP_SERVERS = "192.168.80.34:9092"
TOPIC = "gittba10_link_VWAP2"
GROUP_ID = "gittba10_link_vwap2_group1"

def main():
    consumer = KafkaConsumer(
        bootstrap_servers=[BOOTSTRAP_SERVERS],
        group_id=GROUP_ID,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        key_deserializer=lambda v: v.decode("utf-8") if v else None,
        value_deserializer=lambda v: v.decode("utf-8") if v else None
    )

    consumer.assign([TopicPartition(TOPIC, 0)])

    print(f"Escuchando topic {TOPIC}...")
    while True:
        records = consumer.poll(timeout_ms=5000)
        for _, consumer_records in records.items():
            for record in consumer_records:
                print("key:       ", record.key)
                print("value:     ", record.value)
                print("offset:    ", record.offset)
                print("timestamp: ", record.timestamp)
                print()

if __name__ == "__main__":
    main()