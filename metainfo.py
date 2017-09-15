from .dbconnection import DBConnection
from sqlalchemy import text
import dmp.assets

class MetaInfo:
    table_meta_info_list = {}

    @staticmethod
    def getMetaInfo(db_conn,table_schema,table_name):
        from .processor import AssetsProcessor

        if not table_name in MetaInfo.table_meta_info_list:
            conn = db_conn.engine.connect()
            result = conn.execute(text("SELECT column_name,data_type from information_schema.columns where table_name=:table_name and table_schema=:table_schema"), {'table_name': table_name,'table_schema': table_schema } )
            meta_columns = []
            for row in result:
                meta_columns.append((row['column_name'],row['data_type']))
            MetaInfo.table_meta_info_list[table_name] = meta_columns

        return MetaInfo.table_meta_info_list[table_name]


