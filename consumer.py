from kafka import KafkaConsumer
import json 

consumer = KafkaConsumer(
    'orders',
    bootstrap_servers ='localhost:9094',
    auto_offset_reset='earliest' ,
    group_id='order_group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))

)

print("Consumer started...\n")

for message in consumer:

    order = message.value

    print(f"""
        Received Order:
        Order ID   : {order['order_id']}
        Customer   : {order['customer']}
        Product    : {order['product']}
        Quantity   : {order['quantity']}
        Price      : {order['price']}
        Timestamp  : {order['timestamp']}
        Partition  : {message.partition}
        Offset     : {message.offset}
        -------------------------------
        """)