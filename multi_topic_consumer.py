from kafka import KafkaConsumer
import json

try:

    consumer = KafkaConsumer(
        'orders',
        'payments',
        'user_activity',

        bootstrap_servers='localhost:9094',

        auto_offset_reset='earliest',

        group_id='analytics-group',

        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print("\nAvailable Topics:")
    print(consumer.topics())

    print("\nSubscribed Topics:")
    print(consumer.subscription())

    consumer.poll(timeout_ms=1000)

    print("\nAssigned Partitions:")
    print(consumer.assignment())

    print("\nConsumer is listening...\n")

    for message in consumer:

        data = message.value

        print("=" * 60)

        print(f"TOPIC      : {message.topic}")
        print(f"PARTITION  : {message.partition}")
        print(f"OFFSET     : {message.offset}")

        print("-" * 60)

        for key, value in data.items():

            print(f"{key.upper():15}: {value}")

        print("=" * 60)

except Exception as e:

    print(f"Error: {e}")