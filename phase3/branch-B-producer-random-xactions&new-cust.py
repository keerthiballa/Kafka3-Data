from time import sleep
from json import dumps
from kafka import KafkaProducer
import time
import random
from faker import Faker

from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
# from sqlalchemy.orm import sessionmaker
# from contextlib import contextmanager
# import yaml
import psycopg2


fake=Faker()

class ProducerB:
    def __init__(self):
        self.producerB = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda m: dumps(m).encode('ascii'))

    def emit(self, cust=55, type="dep"):
        data = {'custid' : random.randint(50,56),
            'type': self.depOrWth(),
            'date': int(time.time()),
            'amt': random.randint(10,101)*100            
            }
        return data

    def depOrWth(self):
        return 'dep' if (random.randint(0,2) == 0) else 'wth'

    def generateRandomXactions(self, n=1000):
        for _ in range(n):
            data = self.emit()
            print('sent', data)

            self.conn = psycopg2.connect(database='zipbank', user='zipbankuser2', password='zipbankuserpassword', host='localhost', port='5432')
            self.cursor=self.conn.cursor()

            self.cursor.execute("create table if not exists customer (custid int not null primary key, fname varchar(250) not null, lname varchar (250) not null, createdate int);")
        
            self.conn.commit()

            # print(self.cursor.execute(f"select count(custid) from customer where custid={data['custid']};"))

            if self.cursor.execute(f"select exists(select 1 from customer where custid={data['custid']});")==None:
                self.producerB.send("bank-customer-new", value={'custid':data['custid'],'fname':fake.first_name(),'lname':fake.last_name(),'createdate':time.time()})
            sleep(3)
                
            self.producerB.send('bank-customer-events-branch-B', value=data)

            sleep(1)

if __name__ == "__main__":
    p = ProducerB()
    p.generateRandomXactions(n=20)
