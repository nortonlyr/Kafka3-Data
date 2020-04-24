from kafka import KafkaConsumer, TopicPartition
from json import loads
import os
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy import Integer, String, Column

mysqlkey = os.environ.get('mysql_key')


class XactionConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer('bank-customer-events',
                                      bootstrap_servers=['localhost:9092'],
                                      # auto_offset_reset='earliest',
                                      value_deserializer=lambda m: loads(m.decode('ascii')))
        self.ledger = {}
        self.custBalances = {}
        self.mysql_engine = create_engine("mysql+pymysql://root:" + mysqlkey + "@localhost/bank-customer-events")
        self.conn = self.mysql_engine.connect()


    def handleMessages(self):
        for message in self.consumer:
            message = message.value
            print('{} received'.format(message))
            self.ledger[message['custid']] = message
            #Create a table in mysql first with id (auto_increment primary key), custid, type, date and amt
            self.conn.execute("INSERT INTO transaction4 VALUES (%s,%s,%s,%s,%s)",
                              (int(), int(message['custid']), str(message['type']), int(message['date']), int(message['amt'])))

            if message['custid'] not in self.custBalances:
                self.custBalances[message['custid']] = 0
            if message['type'] == 'dep':
                self.custBalances[message['custid']] += message['amt']
           

            else:
                self.custBalances[message['custid']] -= message['amt']
            print(self.custBalances)


if __name__ == "__main__":
    c = XactionConsumer()
    c.handleMessages()