from kafka import KafkaProducer
import json
import time
from datetime import datetime

producer = KafkaProducer(

    bootstrap_servers = 'localhost:9094',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


# order_id = 1

# while True:

#     order={
#         "order_id": order_id,
#         "customer": f"customer_{order_id}",
#         "product": "Laptop",
#         "quantity": 1,
#         "price": 1000,
#         "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#     }


#     producer.send("orders", value=order)
#     print(f"Sent: {order}")
#     order_id += 1
#     time.sleep(2)

with open ("orders.json","r") as file:

    orders = json.load(file)

    for order in orders:

        producer.send("orders", value=order)

        print(f"sent:{order}....")

        time.sleep(2)

producer.flush()

print("All orders messages sent successfully...")