# Phase 2

# Build two "analytical" consumers.Two, build a "limit" watcher. This is a made-up idea, but the idea is watch for accounts that exceed a certain negative number, say -5000, and print an error message when that happens.

# Unless you need to, don't bother to store any of the output or state in the SQL DB, just keep it in memory.

# LimitConsumer

# LimitConsumer should keep track of the customer ids that have current balances greater or equal to the limit supplied to the constructor. The intro suggests -5000 for eaxmple, but you should be able set that with a parameter to the class' Constructor

from kafka import KafkaConsumer, TopicPartition
from json import loads

class XactionConsumer:
    def __init__(self, limit=-5000):
        self.consumer = KafkaConsumer('bank-customer-events',
            bootstrap_servers=['localhost:9092'],
            # auto_offset_reset='earliest',
            value_deserializer=lambda m: loads(m.decode('ascii')))
        ## These are two python dictionarys
        # Ledger is the one where all the transaction get posted
        self.ledger = {}
        # custBalances is the one where the current blance of each customer
        # account is kept.
        self.custBalances = {}
        # THE PROBLEM is every time we re-run the Consumer, ALL our customer
        # data gets lost!
        # add a way to connect to your database here.
        #Go back to the readme.
        self.transactions={}
        self.limit=limit

    def handleMessages(self):
        for message in self.consumer:
            message = message.value
            print('{} received'.format(message))
            self.ledger[message['custid']] = message
            # add message to the transaction table in your SQL usinf SQLalchemy
            if message['custid'] not in self.custBalances:
                self.custBalances[message['custid']] = 0
            if message['type'] == 'dep':
                self.custBalances[message['custid']] += message['amt']
            else:
                self.custBalances[message['custid']] -= message['amt']
                if(self.custBalances[message['custid']]<=self.limit):
                    print(f"Account balance for  customer with id# {message['custid']} is negative and below {self.limit}. Sufficient balance needs to be maintained in account")
            print(self.custBalances)

if __name__ == "__main__":
    c = XactionConsumer()
    c.handleMessages()
    
