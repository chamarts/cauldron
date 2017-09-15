import sys
import math

from abc import ABCMeta
from .constants import Constants
from .chunkeddbsink import ChunkedSink

from sqlalchemy import text
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import ProcessPoolExecutor

##abstract base class for SinkTargets
class SinkTarget:
    __metaclass__ = ABCMeta

    def sink(self):
        pass

class DBSink(SinkTarget):

    def __init__(self,**params):
        self.table_name = params['table_name']
        self.chunks = DBSink.__chunks(params['data'],Constants.CHUNK_SIZE)
        self.db_conn = params['db_conn']

        #self.executor = ProcessPoolExecutor(max_workers=5)#
        self.executor = ThreadPoolExecutor(max_workers=5)

        self.schema_name = params['schema_name']
        self.include_columns = params['include_columns']

        self.insert_stmt = 'insert into ads.' + self.table_name + ' (' + ','.join(
                            self.include_columns) + ') VALUES (' + ','.join(map(lambda x: ':' + x, self.include_columns)) + ')'

        if 'truncate' in params:
            if params['truncate']:
                try:
                    conn = self.db_conn.connect()
                    result = conn.execute(text('delete from '+ self.schema_name + '.' + self.table_name))
                except:
                    print("Unexpected error:", sys.exc_info()[0])
                    raise
                finally:
                    conn.close()

    def __del__(self):
        self.executor.shutdown(wait=True)

    def sink(self):
        if self.chunks:
            try:
                for chunk in self.chunks:
                    chunkedSink = ChunkedSink(self.db_conn,self.insert_stmt, chunk)
                    self.executor.submit(chunkedSink.sink)
            except:
                print("Unexpected error:", sys.exc_info()[0])
                raise

        return "OK"

    def __chunks(l, n):
        """Yield successive n-sized chunks from l."""
        for i in range(0, len(l), n):
            yield l[i:i + n]
