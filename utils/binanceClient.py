from binance.client import Client as binanceClient
from utils.utility import getMYSQLConnection
import time
import pandas as pd

class binance():
    
    def __init__(self,conf):

        binanceConf            = conf["binance"]
        self.binanceClient     = binanceClient(binanceConf["api"],
                                               binanceConf["secret"])
        self.conf              = conf
                
    def gatherData(self,pairList):
        try:
            while True:
                for pair in pairList:
                    self.getStats(pair)
                    self.getOrderBook(pair)
                print("data gathering completed")
                #run once every 24 hours
                time.sleep(60*60*24)
        except KeyboardInterrupt:
            print("data capture exited succesfully!")
        
        except Exception as e:
            print(f"error seen while running data capture {e}")
                
    def getStats(self,pair):

        stats           = self.binanceClient.get_ticker(symbol=pair)
        df              = pd.DataFrame(stats,index=[0])
        df              = df[["weightedAvgPrice","volume"]].copy()
        df["pair"]      = pair
        df["timestamp"] = time.time()
        
        with getMYSQLConnection(self.conf) as con:
            df.to_sql(name="pair_statistics",con=con,if_exists='append',index=False)
        
    def getOrderBook(self,pair):
        
        depth = self.binanceClient.get_order_book(symbol=pair,
                                                  limit=5000)

        bidsDF              = pd.DataFrame(depth["bids"],columns=["price","volume"])
        bidsDF.price        = bidsDF.price.astype(float)
        
        asksDF              = pd.DataFrame(depth["asks"],columns=["price","volume"])
        asksDF.price        = asksDF.price.astype(float)

        bidsDF["side"] = "bids"
        asksDF["side"] = "asks"
                
        df = pd.concat([bidsDF,asksDF],axis=0)
        df["timestamp"] = time.time()
        df["pair"]      = pair
        
        with getMYSQLConnection(self.conf) as con:
            df.to_sql(name="order_book",con=con,if_exists='append',index=False)
        
#%%    
if __name__ == '__main__':
    bi = binance(conf=conf,tickerList=['ETHUSDT'])