import argparse
from argparse import RawTextHelpFormatter
from utils.binanceClient import binance
from utils.mailSender import mailClass
import utils.dbManager as db
from utils.utility import parse_config

conf         = parse_config(r"config/conf.yaml")
sqlConf      = parse_config(r"config/sql.yaml")

#%%Arg Parse
if __name__ == '__main__':

    description = \
    '''
     TO DO 
    '''
    
    parser = argparse.ArgumentParser(description=description,formatter_class=RawTextHelpFormatter)

    parser.add_argument("-r",
                        "-run",
                        type=str,
                        required=True,
                        choices=['init','populate','data','output'],
                        help='''WIP''')

    parser.add_argument("--d",
                        "--dates",
                        type=str,
                        nargs='+',
                        required=False,
                        help="enter the date lower and upper bounds 12-mar-2021 13-mar-2021")

    parser.add_argument("--t",
                        "--tickers",
                        type=str,
                        nargs='+',
                        required=False,
                        help="enter the ticker list seperated by spaces, example ETHUSDT")

    parser.add_argument('--m',
                       '--table',
                       type=str,
                       help="Enter Name of SQL Table to send by mail") 

    args = parser.parse_args()
    
    
    if args.r == 'init':
        dbManager = db.dbManager(conf=conf)
        dbManager.initializeDB()
        print("DB created and all tables refreshed")

    elif args.r == 'populate':
        dbManager = db.dbManager(conf=conf)
        dbManager.generateMissingTables()
        print("All tables refreshed")

    elif args.r == 'data':
        
        if not (args.t):
            parser.error('''Need to specify the tickers''')

        bi = binance(conf=conf)
        bi.gatherData(args.t)
    
    elif args.r == 'output':
        print("doing output")

    elif args.r == 'mail':
        if not (args.m):
            parser.error('''Need to specify the table name to send by mail''')
        mail = mailClass(conf=conf)
        mail.sendMail(body=f"Your table {args.m} is attached",
                      subject=f"table {args.m}",
                      tbName=args.m)

    elif args.r == 'import':
        if not (args.m):
            parser.error('''Need to specify the table to import''')
        dbManager = db.dbManager(conf=conf)
        dbManager.importTable(args.m)

    else:
        print("doing nothing")