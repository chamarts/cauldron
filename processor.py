from .cauldron import Kettle
from .transformation import Transformation
from .sqlscriptstep import SQLScriptStep
from .dbsinkstep import DBSinkStep
from configparser import ConfigParser
from .dbconnection import DBConnection
import datetime
from .constants import Constants
import sys
from .sinktarget import DBSink
import time

class AssetsProcessor :

    connections = {}

    def __init__(self,full_or_delta=True):

        print('Initializing Assets Procecssor............')

        #read config files and load configuration
        self.config = ConfigParser()
        self.asset_config = ConfigParser()
        self.config.read(['db.cfg'])
        self.asset_config.read('assets.cfg')

        #initialize database connections
        self.__initDBConnections()

        if full_or_delta:
            self.last_updated_time = self.__getJobLastRunTime()
            print("Last update date is set to {0}".format(self.last_updated_time))
        else:
            self.last_updated_time = self.asset_config.get('defaults', 'last_updated_time')
            print("Last update date is set to {0}".format(self.last_updated_time))

        self.buildTransformations()


    ''' build sync and transformation jobs '''
    def buildTransformations(self):
        #setup sync job
        self.syncKettle = Kettle("Sync Job")
        self.__initSync()

        #setup transformation job
        self.transKettle = Kettle("Transformation Job")
        self.__initTrans()

        #register listener for transformation job
        self.syncKettle.register(self.transform)

    def start(self):
        #start exeucting jobs. GOO!!!
        print('Starting AssetsProcecssor..................')
        self.syncKettle.start()

    def transform(self):
        self.transKettle.start()

    #factory method to return reusuable database connections by name (siebel -> oracle connection, dmp-> postgres)
    @staticmethod
    def getDBConnection(conn_name):
        return AssetsProcessor.connections[conn_name]

    ## ----------------AssetProcessor Initialization Helpers ---------------------------------
    def __initSync(self):

        job_start_date = datetime.datetime.now()

        ''' for all table names defined in sync section of assets.cfg file'''
        for table_name in self.asset_config.get('sync','table.names').split(','):

            table_name = table_name.strip()
            t = Transformation(table_name.strip())

            select_stmt = 'select * from ' + self.config.get(Constants.SIEBEL_DB_STRING,'schema') \
                          + '.' + table_name + ' where last_upd between to_timestamp(\'' \
                          + self.last_updated_time.strftime("%Y-%m-%d %H:%M:%S") +'\',\''+ Constants.TIMESTAMP_FORMAT + '\')' + \
                          ' AND to_timestamp(\'' + job_start_date.strftime("%Y-%m-%d %H:%M:%S") +'\',\'' + Constants.TIMESTAMP_FORMAT + '\')'

            sourceFetchStep = SQLScriptStep(db_conn=self.getDBConnection(Constants.SIEBEL_DB_STRING), name="SQLStep:ToFetchSourceTableData:"+ table_name+":",
                          sql_stmt=select_stmt)

            sinkStep = DBSinkStep(tableName=table_name, schemaName=self.config.get(Constants.DMP_DB_STRING, 'schema'),
                       dbConn=self.getDBConnection(Constants.DMP_DB_STRING), name="DBSinkStep:ToWriteToStagingTable:" + table_name +":")

            t.addStep(sourceFetchStep).addStep(sinkStep)

            self.syncKettle.add(t)


    '''Initialize Transaction Job'''
    def __initTrans(self):
        ''' for all table names defined in sync section of assets.cfg file'''
        for table_name in self.asset_config.get('sql').split(','):
            t = Transformation("S_ASSET_XA_TERM")

            t.addStep(SQLScriptStep(db_conn=self.getDBConnection(Constants.SIEBEL_DB_STRING),
                                    sql_stmt="select * from SIEBEL.S_BU where (sysdate -last_upd) < 1"))
            t.addStep(DBSinkStep(table_name='S_BU',schema_name=self.config.get(Constants.SIEBEL_DB_STRING,'schema'),
                              db_conn=self.getDBConnection(Constants.DMP_DB_STRING)))

            self.transKettle.add(t)

    '''Initialize all required database connections'''
    def _initDBConnections(self):
        for section in self.config.sections():
            db_conn = DBConnection(db_url=self.config.get(section,'db.url'))
            AssetsProcessor.connections[section] = db_conn

    '''get last successful job execution time'''
    def __getJobLastRunTime(self):

        try:
            conn = AssetsProcessor.getDBConnection(Constants.DMP_DB_STRING).engine.connect()
            result = conn.execute('select max(last_upd) as last_updated_time from ads.job_audit where status=1')
            return result.fetchone()[0]
        except:
            print("Unexpected error:", sys.exc_info()[0])
            raise
        finally:
            conn.close()
