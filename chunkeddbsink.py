from sqlalchemy import text
import sys

class ChunkedSink:


    def __init__(self, db_conn, sql_stmt, chunked_rows):
        self.chunked_rows = chunked_rows
        self.db_conn = db_conn
        self.sql_stmt = sql_stmt


    '''Be a man! do the right thing !!!'''
    def sink(self):

        try:
            conn = self.db_conn.connect()
            trans = conn.begin()
            for idx, row in enumerate(self.chunked_rows):
                #print('inserting row::{0}'.format(idx))
                result = conn.execute(text(self.sql_stmt), **row)
                print('inserted {0} records into '.format(result.rowcount))
            trans.commit()

        except:
            print("Unexpected error:", sys.exc_info()[0])
            trans.rollback()
            raise
        finally:
            conn.close()