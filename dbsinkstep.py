from .step import Step
from .sinktarget import DBSink
from .metainfo import MetaInfo

class DBSinkStep(Step):

    def __init__(self,**params):
        super().__init__(params['name'])
        self.table_name = params['table_name']
        self.db_conn = params['db_conn']
        self.schema_name = params['schema_name']

    def perform(self):
        dbSink = DBSink(table_name=self.table_name,schema_name=self.schema_name,
                        db_conn=self.db_conn,
                        data=self.data,
                        truncate=True,
                        include_columns=tuple(x[0] for x in MetaInfo.getMetaInfo(self.db_conn,self.schema_name,self.table_name)))
        dbSink.sink()
