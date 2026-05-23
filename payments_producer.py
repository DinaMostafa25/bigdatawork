from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9094',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

with open("payments.json", "r") as file:

    payments = json.load(file)

    for payment in payments:

        producer.send("payments", value=payment)

        print(f"Sent Payment: {payment}")

        time.sleep(2)

producer.flush()

print("All payments messages sent successfully.")