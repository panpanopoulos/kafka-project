import json
from kafka import KafkaProducer
from kafka import KafkaConsumer
import random
import time
import psycopg2 as pg2
import datetime
from datetime import timedelta, datetime

KAFKA_TOPIC = 'food_orders'
PRODUCER_LIMIT_MINS = 2

menu = [{'product': 'cheeseburger', 'price': 15}, {'product': 'hotdog', 'price': 10},
        {'product': 'coca-cola', 'price': 3}, {'product': 'pizza', 'price': 12}, {'product': 'fanta', 'price': 3}]

producer = KafkaProducer(bootstrap_servers=['localhost:29092'])

start = datetime.now()
minutes_passed = 0
order_num = 0
while minutes_passed < PRODUCER_LIMIT_MINS:
    product_ordered = random.choice(menu)
    producer.send(KAFKA_TOPIC, json.dumps(product_ordered).encode("utf-8"))
    print('order #{} is sent'.format(order_num))
    time.sleep(10)
    time_elapsed_timedelta = datetime.now() - start
    minutes_passed = time_elapsed_timedelta.total_seconds() // 60
    order_num += 1
