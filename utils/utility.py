from sqlalchemy import create_engine
import yaml
import datetime
import os
#%%
def parse_config(path):
    with open(path, 'r') as stream:
        return  yaml.load(stream, Loader=yaml.FullLoader)

#%%
def hexToInt(x):
    return int(x, 16)

def killFile(fpath):
    if os.path.exists(fpath):
        os.remove(fpath)
                
#%%
def getMYSQLConnection(conf,db='crypto_liquidity'):
    sqlConf =conf.get('mysql').get(db)
    engine_string = 'mysql+pymysql://{0}:{1}@{2}/{3}'.format(sqlConf["user"],sqlConf["password"],sqlConf["host"],sqlConf["database"])
    engine = create_engine(engine_string)
    con = engine.connect()
    return con


#%%Date Conversions
def getTimestampFromString(dateString):
    return int(datetime.datetime.strptime(dateString, '%d-%b-%Y').timestamp())

def getDateFromTimestamp(timestamp):
    return datetime.datetime.utcfromtimestamp(timestamp).strftime('%d-%b-%Y')
