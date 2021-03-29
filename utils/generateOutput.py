from utils.utility import getMYSQLConnection, getTimestampFromString
import pandas as pd

class processData():
    def __init__(self,conf,sqlConf):
        self.conf    = conf
        self.sqlConf = sqlConf
        
        #bucket ranges in bp
        self.factorList = list(range(10,110,10))
        self.factorList.extend([150,200])
        self.factorList.extend(range(200,1200,200))
        
        #Bid/Ask specific functions
        self.bidAskSpec = {'bids':{'bestPrice':min,
                                   'manipulationFactor':1},
                           'asks':{'bestPrice':max,
                                   'manipulationFactor':-1}}
                
    
    def generateOutput(self,startDate,endDate,pairList):

        self.startTime = getTimestampFromString(startDate)
        self.endTime   = getTimestampFromString(endDate)
            
    def orderBookStats(self,pairList):
        
        finalResults = pd.DataFrame()
        
        for pair in pairList:
            #get orderbook
            df = self.getTableFromSQL(self.sqlConf["getOrderBook"].format(pair,
                                                                          self.startTime,
                                                                          self.endTime))
            df["pxv"] = df["price"].multiply(df["volume"])
            
            #loop on bid/ask
            for side in df.side.unique():
                
                #Filter side
                sideDF = df.query("side == @side").copy()
                
                #Get Bid Ask Transformation Function To get Best bid / ask from vector of prices
                #For bid it's min, for ask it's max            
                bidAskFunction = self.bidAskSpec[side]['bestPrice']
                
                #Gets best bid/ask per timestamp, for bids it's min, for asks it's max
                bestPriceDict = sideDF.groupby("timestamp")["price"].aggregate(bidAskFunction).to_dict()
                
                #Add the best price to the sideDF
                sideDF["bestPrice"] = sideDF["timestamp"].map(bestPriceDict)
                
                #Manipulation Factor is a binaary variable that pushes prices up
                #for bids (buying the lot) and down for asks  (selling the lot)
                manipulationFactor = self.bidAskSpec[side]["manipulationFactor"]
                
                #Push the price by the manipulation factor
                for factor in self.factorList:
                    
                    sideDF["factorPrice"] = sideDF["bestPrice"] * ( 1 + factor/1e4 * manipulationFactor )
                    
                    #Filter prices that are within the factor range
                    finalDF = sideDF[((sideDF["price"] - sideDF["factorPrice"]) * manipulationFactor < 0)]
                    
                    #Get the $ that needs to be spent per timestamp to manipulate the price by a given factor
                    finalDF = finalDF.groupby('timestamp')["pxv"].sum()
                    
                    #Get the stats on those $s that need to be spent
                    stats = finalDF.aggregate(['mean','std','median']).to_dict()
                    
                    tempResults = pd.DataFrame({'side':side,'factor':factor,'pair':pair,'stats':stats})
                    
                    #Save the results
                    finalResults = pd.concat([tempResults,finalResults],axis=0)
            
        return finalResults
        
    def getTableFromSQL(self,sql):
        with getMYSQLConnection(self.conf) as con:
            return pd.read_sql(sql,con)
        
        
#%%
if __name__ == '__main__':

    data = processData(conf=conf, sqlConf=sqlConf)