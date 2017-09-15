import unittest
from dmp.assets.sinktarget import DBSink
from dmp.assets.processor import *
from dmp.assets.metainfo import MetaInfo

class DBSinkTest(unittest.TestCase):

    def test_sink(self):
        url = 'postgresql+psycopg2://dmp:dmp@ch3lxesgdi08.corp.equinix.com:5432/dmp2'
        db_conn = DBConnection(dbUrl=url)
        table_name = "asset_xa"
        DBSink(dbConn=db_conn,
               truncate=True,
                data=[{'row_id':1, 'created_by':'chamarts','last_upd_by':'chamarts'}],
                schemaName='ads',tableName="test",include_columns=tuple(x[0] for x in MetaInfo.getMetaInfo(db_conn,table_name))).sink()
