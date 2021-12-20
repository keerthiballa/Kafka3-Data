# from sqlalchemy.ext.declarative import declarative_base
# from sqlalchemy import Column, Integer, String, Date
# Base=declarative_base()

# class Transaction(Base):
#     __tablename__ = 'transactions'
#     # Here we define columns for the table person
#     # Notice that each column is also a normal Python instance attribute.
#     id = Column(Integer, primary_key=True)
#     custid = Column(Integer)
#     type = Column(String(250), nullable=False)
#     date = Column(Integer)
#     amt = Column(Integer)

#     # def __repr__(self) -> str:
#     #     return super().__repr__()

#     def __repr__(self):
#         return "<Transaction(id={},custid={},type='{}',date={},amt={})>.format(self.id,self.custid,self.type,self.date,self.amt)"