from sqlalchemy import create_engine
import sys

class DBConnection:

    def __init__(self,**params):
        try:
            self.engine = create_engine(params['db_url'])
        except:
            print("Unexpected error:", sys.exc_info()[0])
            raise

    def connect(self):
        return self.engine.connect()

    def close(self):
        self.engine.close()

    def __del__(self):
        self.engine.close

