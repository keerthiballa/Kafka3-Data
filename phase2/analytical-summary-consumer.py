# Phase 2

# Build two "analytical" consumers. One, build a consumer that everytime it starts, produces an on-going statistical summary of all the transactions seen by the system. 

# Unless you need to, don't bother to store any of the output or state in the SQL DB, just keep it in memory.

# SummaryConsumer

# SummaryConsumer should produce a list of outputs, the status of the mean (avg) deposits and mean withdrawals across all customers. You should also print the standard deviation of the distribution for both deposits and withdrawals. As each transaction comes in, print a new status of the numerical summaries.



from kafka import KafkaConsumer, TopicPartition
from json import loads
from statistics import *

class XactionConsumer:
    def __init__(self):
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
        self.run_mean_dep=0
        self.run_mean_wth=0
        self.run_std_dev_dep=0
        self.run_std_dev_wth=0
        self.transactions={}
        self.i=0
        self.sum_dep=0
        self.sum_wth=0
        self.num_dep=0
        self.num_wth=0
        self.num_stdev_dep=0
        self.num_stdev_wth=0
        

    def handleMessages(self):
        for message in self.consumer:

            message = message.value
            print('{} received'.format(message))
            
            self.ledger[message['custid']] = message
            self.transactions[self.i]=message

            # print(self.transactions)
            # print(type(self.transactions))
        

            # add message to the transaction table in your SQL usinf SQLalchemy
            if message['custid'] not in self.custBalances:
                self.custBalances[message['custid']] = 0
            if message['type'] == 'dep':
                self.custBalances[message['custid']] += message['amt']
                self.num_dep += 1
                self.sum_dep =self.sum_dep + self.transactions[self.i]['amt']
                self.run_mean_dep=self.sum_dep/self.num_dep
                self.num_stdev_dep=self.num_stdev_dep + (self.run_mean_dep - self.transactions[self.i]['amt'])**2
                self.run_std_dev_dep=(self.num_stdev_dep/self.num_dep)**.5
            else:
                self.custBalances[message['custid']] -= message['amt']
                self.num_wth += 1
                self.sum_wth =self.sum_wth + self.transactions[self.i]['amt']
                self.run_mean_wth=self.sum_wth/self.num_wth
                self.num_stdev_wth=self.num_stdev_wth + (self.run_mean_wth - self.transactions[self.i]['amt'])**2
                self.run_std_dev_wth=(self.num_stdev_wth/self.num_wth)**.5
            print(self.custBalances)
            print("Deposits: Running Mean is ", round(self.run_mean_dep,2), "Running Std. Dev. is ", round(self.run_std_dev_dep,2))
            print("Withdrawals: Running Mean is ",round(self.run_mean_wth,2), "Running Std. Dev. is ", round(self.run_std_dev_wth,2))
            
            self.i+=1
            

if __name__ == "__main__":
    c = XactionConsumer()
    c.handleMessages()
    
