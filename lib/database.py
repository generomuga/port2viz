from os import error
import sqlite3 as sq
import sys

class Database:

    def __init__(self):
        pass

    def connect(self, **kwargs):
        db = kwargs['db']
        try:
            print ('Connected to db: '+db+'...')
            return sq.connect(db)
        except Exception as err:
            print (err)

    def get_data(self, **kwargs):
        cur = kwargs['cursor']
        query = kwargs['query_str']
        try:
            print ('Successfully get the data...')
            return cur.execute(query)
        except Exception as err:
            print (err)

    def post_data(self, **kwargs):
        con = kwargs['con']
        cur = kwargs['cursor']
        query = kwargs['query_str']
        data = kwargs['data']
        
        try:
            print ('Successfully saved the data...')
            cur.executemany(query,data)
            con.commit()    
        except Exception as err:
            print (err)
            con.rollback()

    def truncate_table(self, **kwargs):
        con = kwargs['con']
        cur = kwargs['cursor']
        table = kwargs['table']
        try:
            query = "DELETE FROM "+table
            print ('Truncate table')
            cur.execute(query)
            con.commit()    
        except Exception as err:
            print (err)
            con.rollback()