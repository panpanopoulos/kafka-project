import json
from kafka import KafkaProducer
from kafka import KafkaConsumer
import random
import time
import psycopg2 as pg2

KAFKA_TOPIC = 'food_orders'
postgres_user = 'postgres'
postgres_password = 'postgres'

consumer = KafkaConsumer(KAFKA_TOPIC, bootstrap_servers=['localhost:29092'])

print('Start receiving the orders')
conn = pg2.connect(database='kafka_project_orders', user=postgres_user, password=postgres_password)
cursor = conn.cursor()
for message in consumer:
    print('Ongoing order')
    consumed_message = json.loads(message.value.decode())
    product = consumed_message['product']
    price = consumed_message['price']
    cursor.execute("INSERT INTO orders(product,price) VALUES ('{}',{})".format(product, price))
    conn.commit()

conn.close()
