from kafka import KafkaConsumer, TopicPartition
from json import loads
import json


class SummaryConsumer:



class LimitConsumer:
    consumer_id = 0

    def __init__(selfs):
        consumer = KafkaConsumer(bootstrap_servers = ['localhost:9092'],
                                 value_deserializer=lambda m: loads(m.decode('ascii'),
                                    group_id = branch_id + 1))

    def handleMessages(self):
        for message in self.consumer:
            message = message.value
            print('{} received'.format(message))
            limit_amt = -5000

            if message['amt'] < limit_amt:
                print('Alert: {} Too much'.format(message))




if __name__ == "__main__":
   consumer_1 = branch_consumer()
   consumer_2 = branch_consumer()

   consumer_1.handleMessages()
   consumer_2.handleMessages()