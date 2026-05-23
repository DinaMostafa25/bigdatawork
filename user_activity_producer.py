from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9094',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

with open("user_activity.json", "r") as file:

    activities = json.load(file)

    for activity in activities:

        producer.send("user_activity", value=activity)

        print(f"Sent Activity: {activity}")

        time.sleep(2)

producer.flush()

print("All user activity messages sent successfully.")