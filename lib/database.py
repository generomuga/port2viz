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
            return cur.execute(query)
        except Exception as err:
            print (err)