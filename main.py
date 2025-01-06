from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
import logging
import uuid
import random
import time
import json
import threading 

KAFKA_BROKERS="localhost:29092,localhost:39092,localhost:49092"
NUM_PARTITIONS = 5
REPLICATION_FACTOR = 3
TOPIC_NAME='financial_transactions'
# logging.basicConfig(
#     level=logging.INFO
# )
logger = logging.getLogger(__name__)

producer_conf={
    "bootstrap.servers": KAFKA_BROKERS,
    'queue.buffering.max.messages': 150000,
    'queue.buffering.max.kbytes': 512000,
    'batch.num.messages': 1000,
    'linger.ms':10,
    'acks':1,
    'compression.type':'gzip'
}

producer= Producer(producer_conf)

def create_topic(topic_name):
    admin_client = AdminClient({"bootstrap.servers":KAFKA_BROKERS})
    try:
        metadata=admin_client.list_topics(timeout=10)
        if topic_name not in metadata.topics:
            topic = NewTopic(
                topic=topic_name,
                num_partitions = NUM_PARTITIONS,
                replication_factor= REPLICATION_FACTOR
            )
            
            fs = admin_client.create_topics([topic])
            for topic, future in fs.items():
                try:
                    future.result()
                    logger.info(f"Topic {topic_name} created successfully !")
                except Exception as e:
                    logger.error(f"Failed to create topic '{topic_name}': {e}")
        else:
            logger.info(f"Topic {topic_name} already exists")
    except Exception as e:
        logger.error(f"Error creating topic : {e}")
        
        
def generate_transaction():
    return dict(
        transactionId = str(uuid.uuid4()),
        userId= f"user_{random.randint(1,100)}",
        amount= round(random.uniform(1000,150000),2),
        transactionTime= int(time.time()),
        merchantId = f"merchant_{random.randint(1,20)}",
        transactionType=random.choice(["purchase","refund","sale"]),
        location = random.choice(["France","Morocco","Spain","USA","China", "UAE"]),
        paymentMethod = random.choice(["credit card","paypal","bank transfer"]),
        isInternational = random.choice(["True","False"]),
        currency=random.choice(["USD","EUR","GBP"])
    )
    
def produce_transaction(thread_id):
    while True:
        transaction= generate_transaction()
        try:
            producer.produce(
                topic=TOPIC_NAME,
                key=transaction['userId'],
                value=json.dumps(transaction).encode("utf-8"),
                on_delivery=delivery_report
            )
            
            print(f'Thread {thread_id} Produced transaction : {transaction}')
            producer.flush()
        except Exception as e:
            print(f"Error sending transaction : {e}")
            
def produce_in_parallel(nb_threads):
    threads=[]
    
    try:
        for i in range(nb_threads):
            thread=threading.Thread(target=produce_transaction, args=(i,))
            thread.daemon= True
            thread.start()
            threads.append(thread)
            
        for thread in threads:
            thread.join()
    except Exception as e:
        print(f"Error Message : {e}")
        
def delivery_report(err, msg):
    
    if err is not None:
        print(f'Delivery failed for record {msg.key()}')
    else:
        print(f"Record {msg.key} successfully produced")
    
if __name__=="__main__":
     
    create_topic(TOPIC_NAME)
    produce_in_parallel(3)

