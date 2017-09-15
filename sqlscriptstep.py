from .step import Step
import sys

class SQLScriptStep(Step):

    def __init__(self,**params):
        super().__init__(params['name'])
        self.db_conn = params['db_conn']
        self.sql_stmt = params['sql_stmt']

    def perform(self):
        print('Executing SQLScript' + self.sql_stmt)
        try:
            connection = self.db_conn.connect()
            result = connection.execute(self.sql_stmt)
            resultSet = []
            for row in result:
                resultSet.append(self.__row2dict(row,result.keys()))
            print("fetched {0} rows in {1}".format(result.rowcount,self.name))
            self.result = resultSet
        except:
            print("Unexpected error:", sys.exc_info()[0])
            raise
        finally:
            connection.close()
        return self.result

    def __row2dict(self,row,columns):
        d = {}
        for column in columns:
            d[column] = getattr(row, column)

        return d
