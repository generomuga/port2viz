from os import error
import sqlite3 as sq
import logging
import sys

class Database:

    def __init__(self):
        pass

    def connect(self, **kwargs):
        db = kwargs['db']
        try:
            logging.info('Connected to db: '+db)
            return sq.connect(db)
        except Exception as err:
            logging.error(err)
            print (err)

    def get_data(self, **kwargs):
        cur = kwargs['cursor']
        query = kwargs['query_str']
        try:
            logging.info('Query executed: '+query)
            return cur.execute(query)
        except Exception as err:
            logging.error(err)
            print (err)

    def post_data(self, **kwargs):
        con = kwargs['con']
        cur = kwargs['cursor']
        query = kwargs['query_str']
        data = kwargs['data']
        
        try:
            cur.executemany(query,data)
            con.commit()
            logging.info('Query executed: '+query)
        except Exception as err:
            con.rollback()
            logging.error(err)
            print (err)
            
    def truncate_table(self, **kwargs):
        con = kwargs['con']
        cur = kwargs['cursor']
        table = kwargs['table']
        try:
            query = "DELETE FROM "+table
            cur.execute(query)
            con.commit()    
            logging.info('Query executed: '+query)
        except Exception as err:
            con.rollback()
            logging.error(err)
            print (err)