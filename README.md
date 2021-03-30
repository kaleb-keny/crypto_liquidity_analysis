# Orderbook Depth Analysis

 The repo contains the tools necessary to perform create the output csv files under the output folder which showcase statistics on the depth of an orderbook. 
 
## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development.  The necessary packages on anaconda would need to be installed (more on that later).


### Prerequisites

The code needs miniconda, as all packages were installed and tested on conda v4.9.2. Installation of miniconda can be done by running the following:

```
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
chmod +x Miniconda3-latest-Linux-x86_64.sh
bash Miniconda3-latest-Linux-x86_64.sh -p $HOME/miniconda3
```

The user would also need a mysql server to hook on to, on a linux based system the mysql server can be installed with the following:

```
sudo apt-get install mysql-server
```


#### Installing Enviroment

Refer to [conda docs](https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html).

The enviroment files are available under conda_env folder. Enviroment setup can be done with the below

##### With the enviroment file
```
conda env create --name crypto_liquidity --file=environment.yml
```

#### Adding API Keys to config/conf.yaml

Binance api and secret needs to be updated. Both needs to be populated and they can be obtained from the following:
-  [binance](https://www.binance.com/en/support/faq/360002502072-How-to-create-API) 

```
sourceData:
    binance:
        api: 'XXXXXXXXXXXXXXXXXXXXXXXX'
        secret: 'XXXXXXXXXXXXXXXXXXXXXXXX'
```

#### Adding Mysql Connection to config/conf.yaml

A mysql database needs to be setup, but in order to connect to the server the required fields such as the user , password and a database name need to be added to the conf file as shown below.

```
mysql:
    crypto_liquidity:
        user:  "root"
        password:  'XXXXXXXXXXXXXXXXXXXXXXXX'
        host:  "localhost"
        database:  "crypto_liquidity"
        raise_on_warnings:  True
```


#### Database Setup

Once the mysql connection configurations set, a database can be created with the required tables by performing the following in an anconda command prompt, pointed to the directory of the repo:

```
python main.py -r init
```

### Fetching Data

The following command could be run to fetch data for a given pair

```
python main.py -r data --t ETHUSDT BTCUSDT
```

### Generating Output

The following command could be run to generate the output files for a given currency pair in a given time range:

```
python main.py -r output --t ETHUSDT BTCUSDT --d 28-Mar-2021 30-mar-2021
```
