from sqlalchemy.sql.ddl import CreateTable
from kafka import KafkaConsumer, TopicPartition
from json import loads

from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
# from sqlalchemy.orm import sessionmaker
# from contextlib import contextmanager
# import yaml
import psycopg2


class NewCustConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer('bank-customer-new',
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
       

        #Go back to the readme.

    def handleMessages(self):
        for message in self.consumer:
            message = message.value
            print('{} received'.format(message))
            self.ledger[message['custid']] = message
            # add message to the transaction table in your SQL usinf SQLalchemy

            if self.cursor.execute(f"select exists(select 1 from customer where custid={message['custid']});")==None:
                self.cursor.execute("insert into customer (custid,fname,lname,createdate) values(%s, %s, %s, %s);", (message['custid'], message['fname'],message['lname'],message['createdate']))
                print(f"new customer with id# {message['custid']} has been added")
            elif self.cursor.execute(f"select count(custid) from customer where custid={message['custid']};")!=0:
                print(f"customer with id# {message['custid']} already exists")
            self.conn.commit()    

if __name__ == "__main__":
    c = NewCustConsumer()
    while True:
        c.handleMessages()
    
