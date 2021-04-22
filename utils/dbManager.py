from sqlalchemy import create_engine
from utils.utility import getMYSQLConnection
import pandas as pd

class dbManager():
    def __init__(self, conf):
        self.conf           = conf
        self.dbName         = list(conf["mysql"].keys())[0]
        self.endMaintenance = False #True on kill

    def initializeDB(self):
        self.createDB()
        self.generateMissingTables()

    def createDB(self):
        sql_conf = self.conf.get('mysql')

        user     = sql_conf[self.dbName]["user"]
        password = sql_conf[self.dbName]["password"]
        host     = sql_conf[self.dbName]["host"]
        db       = sql_conf[self.dbName]["database"]

        #Check if Database is there, deletes it
        try:
            engine_string = f'mysql+pymysql://{user}:{password}@{host}'
            engine = create_engine(engine_string)
            drop_db_sql = f'DROP DATABASE IF EXISTS {db};'
        except Exception as e:
            print(e)
            engine_string = f"mysql+pymysql://{user}:{password}@{host}/{db}"
            engine = create_engine(engine_string)

        with engine.connect() as con:
            con.execute(drop_db_sql)
            con.execute(f"CREATE DATABASE {db};")
            con.execute(f"USE {db};")

    def generateMissingTables(self):

        with getMYSQLConnection(self.conf,self.dbName) as con:

            availableTablesList = pd.read_sql("show tables;",con).iloc[:,0].to_list()

            if not "order_book" in availableTablesList:
                sql=\
                '''
                CREATE table order_book
                (
                pair VARCHAR(24),
                timestamp INT(11) UNSIGNED,
                side VARCHAR(12),
                price DECIMAL(30,20),
                volume DECIMAL(30,20),
                CONSTRAINT order_book PRIMARY KEY (pair,timestamp,side,price),
                INDEX (pair,timestamp,side)
                );
                '''
                con.execute(sql)


            if not "pair_statistics" in availableTablesList:
                sql= \
                '''
                CREATE TABLE pair_statistics
                    (
                    pair VARCHAR(24),
                    timestamp INT(11) UNSIGNED,
                    weightedAvgPrice DECIMAL(30,20),
                    volume DECIMAL(50,20),
                     CONSTRAINT pk_p_stat PRIMARY KEY (pair,timestamp),
                     INDEX (pair,timestamp)
                     );
                '''
                con.execute(sql)
                

    def importTable(self,tbName):
        '''
        import table into sql
        

        Parameters
        ----------
        tbName : str
            table name should match the table on the server

        Returns
        -------
        None.

        '''
        df = pd.read_csv(r"input\{}.csv".format(tbName))
        df.drop(columns=[df.columns[0]],inplace=True)
        with getMYSQLConnection(self.conf) as con:
            con.execute("DROP TABLE IF EXISTS temp;")
            df.to_sql("temp",index=False,con=con)
            con.execute(f"INSERT IGNORE INTO {tbName} (select * from temp);")
            con.execute("DROP TABLE IF EXISTS temp;")
        print(f"Table {tbName} has been imported into sql server.")
            
        
                    
#%%
if __name__=='__main__':
    db = dbManager( conf)
    db.initializeDB()
