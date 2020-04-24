from kafka import KafkaConsumer, TopicPartition
from json import loads
import statistics


class XactionConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer('bank-customer-events',
                                      bootstrap_servers=['localhost:9092'],
                                      # auto_offset_reset='earliest',
                                      value_deserializer=lambda m: loads(m.decode('ascii')))
        self.ledger = {}
        self.custBalances = {}
        self.dep_total = 0
        self.dep_counter = 0
        self.dep_avg = 0
        self.dep_list= []
        self.wth_total = 0
        self.wth_counter = 0
        self.wth_avg = 0
        self.wth_list = []

    def handleMessages(self):
        dep_stdev = 0
        wth_stdev = 0
        for message in self.consumer:
            message = message.value
            print('{} received'.format(message))

            self.ledger[message['custid']] = message

            if message['custid'] not in self.custBalances:
                self.custBalances[message['custid']] = 0

            if message['type'] == 'dep':
                self.custBalances[message['custid']] += message['amt']
                self.dep_total += message['amt']
                self.dep_counter += 1
                self.dep_avg = round(self.dep_total / self.dep_counter, 2)
                self.dep_list.append(message['amt'])

                if len(self.dep_list) > 1:
                    dep_stdev = round(statistics.stdev(self.dep_list), 2)

            else:
                self.custBalances[message['custid']] -= message['amt']
                self.wth_total += message['amt']
                self.wth_counter += 1
                self.wth_avg = round(self.wth_total / self.wth_counter, 2)
                self.wth_list.append(message['amt'])

                if len(self.wth_list) > 1:
                    wth_stdev = round(statistics.stdev(self.wth_list),2)

            print(self.custBalances)
            print('dep_avg:', self.dep_avg, 'dep_stdev:', dep_stdev,'wth_avg:', self.wth_avg , 'wth_stdev:', wth_stdev)



if __name__ == "__main__":
    c = XactionConsumer()
    c.handleMessages()