from sqlalchemy.sql.ddl import CreateTable
from kafka import KafkaConsumer, TopicPartition
from json import loads

from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager
# import yaml
import psycopg2


class XactionConsumerB:
    def __init__(self):
        self.consumer = KafkaConsumer('bank-customer-events-branch-B',
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

        self.conn = psycopg2.connect(database='zipbank', user='zipbankuser2', password='zipbankuserpassword', host='localhost', port='5432')
        
        self.cursor=self.conn.cursor()
        self.cursor.execute("create table if not exists transactionB (id SERIAL not null primary key, custid int, type varchar(250) not null, date int, amt int);")
        self.conn.commit()

        #Go back to the readme.

    def handleMessages(self):
        for message in self.consumer:
            message = message.value
            print('{} received'.format(message))
            self.ledger[message['custid']] = message
            # add message to the transaction table in your SQL usinf SQLalchemy

            self.cursor.execute("insert into transactionB (custid,type,date,amt) values(%s, %s, %s, %s);", (message['custid'], message['type'],message['date'],message['amt']))
            self.conn.commit()

            if message['custid'] not in self.custBalances:
                self.custBalances[message['custid']] = 0
            if message['type'] == 'dep':
                self.custBalances[message['custid']] += message['amt']
            else:
                self.custBalances[message['custid']] -= message['amt']
            print(self.custBalances)


if __name__ == "__main__":
    c = XactionConsumerB()
    c.handleMessages()
    
