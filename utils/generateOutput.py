from utils.utility import getMYSQLConnection, getTimestampFromString
import pandas as pd

class processData():
    def __init__(self,conf,sqlConf):
        self.conf    = conf
        self.sqlConf = sqlConf
        
        #bucket ranges in bp
        self.factorList = list(range(25,110,25))
        self.factorList.extend([150,200])
        self.factorList.extend(range(400,1200,200))
        
        #Bid/Ask specific functions
        self.bidAskSpec = {'bids':{'bestPrice':max,
                                   'manipulationFactor':-1},
                           'asks':{'bestPrice':min,
                                   'manipulationFactor':+1}}
                    
    def generateOutput(self,pairList,startDate,endDate):

        startTime = getTimestampFromString(startDate)
        endTime   = getTimestampFromString(endDate)
        
        #Get Order book stats
        dailyTurnover = self.getPairStats(pairList=pairList,
                                          startTime=startTime, 
                                          endTime=endTime)
        
        #dump to csv
        dailyTurnover.to_csv(r"output/dailyTurnover.csv")
        
        #Get bid ask spread
        bidAskSpread = self.getBidAskSpread(pairList=pairList, 
                                            startTime=startTime, 
                                            endTime=endTime)
        #dump to csv
        bidAskSpread.to_csv(r"output/bidAskSpread.csv")
        
        #Get order book slippage  stats
        orderBookStats = self.getOrderBookStats(pairList=pairList, 
                                                startTime=startTime, 
                                                endTime=endTime)
        
        #convert to pivot table
        pivot = pd.pivot_table(data=orderBookStats,
                               values=['stats'],
                               index=['pair','side','stat'],
                               columns='factor',
                               aggfunc='mean',
                               fill_value=0)
        #Save as pivot table
        pivot.to_csv(r"output/orderBookStats.csv")  
        
        
    def getBidAskSpread(self,pairList,startTime,endTime):
        pairString = ''
        for pair in pairList:
            pairString = pairString + ",'" +pair + "'"

        df = self.getTableFromSQL(self.sqlConf["getBidAskSpread"].format(
                                                                         startTime,
                                                                         endTime,
                                                                         pairString[1:],
                                                                         startTime,
                                                                         endTime,
                                                                         pairString[1:]
                                                                         ))
        return df
            
                
    def getOrderBookStats(self,pairList,startTime,endTime):
        
        orderBookResults = pd.DataFrame()
        
        for pair in pairList:
            
            #get orderbook
            df = self.getTableFromSQL(self.sqlConf["getOrderBook"].format(pair,
                                                                          startTime,
                                                                          endTime))
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
                    orderBookResults = pd.concat([tempResults,orderBookResults],axis=0)
            
        orderBookResults.reset_index(inplace=True,drop=False)
        orderBookResults.rename(columns={'index':'stat'},inplace=True)

        return orderBookResults
        
    def getPairStats(self,pairList,startTime,endTime):
        pairString = ''
        for pair in pairList:
            pairString = pairString + ",'" +pair + "'"

        df = self.getTableFromSQL(self.sqlConf["getPairStats"].format(startTime,
                                                                      endTime,
                                                                      pairString[1:]))
        
        return df
    
    def getTableFromSQL(self,sql):
        with getMYSQLConnection(self.conf) as con:
            return pd.read_sql(sql,con)
        
        
#%%
if __name__ == '__main__':

    data = processData(conf=conf, sqlConf=sqlConf)
