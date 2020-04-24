from kafka import KafkaConsumer, TopicPartition
from json import loads
import os
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy import Integer, String, Column

mysqlkey = os.environ.get('mysql_key')
Base = declarative_base()


class Transaction(Base):
    __tablename__ = 'transaction2'
    # Here we define columns for the table person
    # Notice that each column is also a normal Python instance attribute.
    id = Column(Integer, primary_key=True)
    custid = Column(Integer)
    type = Column(String(250), nullable=False)
    date = Column(Integer)
    amt = Column(Integer)


class XactionConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer('bank-customer-events',
                                      bootstrap_servers=['localhost:9092'],
                                      # auto_offset_reset='earliest',
                                      value_deserializer=lambda m: loads(m.decode('ascii')))
        self.ledger = {}
        self.custBalances = {}
        self.mysql_engine = create_engine('mysql+pymysql://root:yourpassword@localhost/bank-customer-events')
        self.conn = self.mysql_engine.connect()
        self.limit = -5000




    def handleMessages(self):
        for message in self.consumer:
            message = message.value
            print('{} received'.format(message))
            self.ledger[message['custid']] = message
            # ch_message = list(message.values())
            # new_values = tuple(ch_message)
            self.conn.execute("INSERT INTO transaction4 VALUES (%s, %s,%s,%s,%s)",
                              (int(), int(message['custid']), str(message['type']), int(message['date']), int(message['amt'])))

            if message['custid'] not in self.custBalances:
                self.custBalances[message['custid']] = 0
            if message['type'] == 'dep':
                self.custBalances[message['custid']] += message['amt']
            if message['type'] == 'wth' and self.custBalances[message['custid']] < self.limit:
                print('withdraw limit reach')

            else:
                self.custBalances[message['custid']] -= message['amt']
            print(self.custBalances)


if __name__ == "__main__":
    c = XactionConsumer()
    c.handleMessages()