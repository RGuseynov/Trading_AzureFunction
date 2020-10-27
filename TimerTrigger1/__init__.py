import os
import datetime
import logging
import pickle

import requests
import pyodbc

import pandas as pd
import xgboost as xgb
import azure.functions as func

from . import features_engineering as fe

# from azure.identity import DefaultAzureCredential
# from azure.identity import ManagedIdentityCredential
# from azure.keyvault.secrets import SecretClient


def main(mytimer: func.TimerRequest) -> None:
    # credential = DefaultAzureCredential()
    # credential = ManagedIdentityCredential()
    # secret_client = SecretClient(vault_url="https://tradingkeyvault.vault.azure.net/", credential=credential)
    # secret = secret_client.get_secret("sqlserver-trading")
    # logging.info(f"secret name is: {secret.name}")

    logging.info('enter function')
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')
    logging.info('Python timer trigger function ran at %s', utc_timestamp)


    try:
        coinAPI_key = os.environ['COIN_API2']
        database_password = os.environ['SQL_SERVER_CONNECTION']
    except Exception as e:
        logging.info(e)

    try:
        url = 'https://rest.coinapi.io/v1/ohlcv/BITSTAMP_SPOT_BTC_USD/latest?period_id=1HRS'
        headers = {'X-CoinAPI-Key' : coinAPI_key}
        response = requests.get(url, headers=headers)
        data = response.json()
        last = data[1]
    except Exception as e:
        logging.info(e)

    cnxn = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};Server=tcp:sqlserver-trading.database.windows.net,1433;Database=financial;Uid=MasterTrader;Pwd="+database_password+";Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;")
    cursor = cnxn.cursor()
    logging.info("connection done")

    try:
        cursor.execute("INSERT INTO bitcoin_1H_bitstampUSD_latest VALUES (?, ?, ?, ?, ?, ?)", 
                last["time_period_end"][:-5], last["price_open"], last["price_high"], last["price_low"],
                last["price_close"], last["volume_traded"])
        cnxn.commit()
        logging.info("price insert done")
    except Exception as e:
        logging.info(e)


    sql_query = ("SELECT TOP(50) * FROM bitcoin_1H_bitstampUSD_latest ORDER BY Timestamp DESC")
    df = pd.read_sql(sql_query, cnxn)
    logging.info("select request done")
    logging.info(df)
    try:
        df = df.iloc[::-1].reset_index()
    except Exception as e:       
        logging.info(e)
    try:
        df = df.drop(['Timestamp', 'index'], axis=1)
    except Exception as e:
        logging.info(e)

    df = fe.add_TA(df)
    logging.info("TA_added")
    logging.info(df)
    last_row = df.tail(1)
    logging.info("last_row_retrieved")

    try:
        with open("TimerTrigger1/xgb-BSH-training-stocks.pkl", "rb") as file:
            xgb_model = pickle.load(file)      
        logging.info("model loaded")
    except Exception as e:
        logging.info(e)

    try:
        last_row = xgb.DMatrix(last_row)
    except Exception as e:
        logging.info(e)

    logging.info("DMatrix created")
    prediction = xgb_model.predict(last_row)

    logging.info("prediction:")
    logging.info(prediction)

    sell = float(prediction[0][0])
    buy = float(prediction[0][1])
    hold = float(prediction[0][2])

    try:
        cursor.execute("INSERT INTO XGBoost_BSH_1H VALUES (?, ?, ?, ?)", 
                last["time_period_end"][:-5], sell, buy, hold)
        cnxn.commit()
        logging.info("prediction insert done")
    except Exception as e:
        logging.info(e)



# url = 'https://rest.coinapi.io/v1/ohlcv/BITSTAMP_SPOT_BTC_USD/latest?period_id=1HRS'
# headers = {'X-CoinAPI-Key' : coinAPI_key}
# response = requests.get(url, headers=headers)
# data = response.json()

# cnxn = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};Server=tcp:sqlserver-trading.database.windows.net,1433;Database=financial;Uid=MasterTrader;Pwd="+database_password+";Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;")
# cursor = cnxn.cursor()
# logging.info("connection done")

# for last in data[1:]:
#     try:
#         cursor.execute("INSERT INTO bitcoin_1H_bitstampUSD_latest VALUES (?, ?, ?, ?, ?, ?)", 
#                 last["time_period_end"][:-5], last["price_open"], last["price_high"], last["price_low"],
#                 last["price_close"], last["volume_traded"])
#         cnxn.commit()
#         logging.info("price insert done")
#     except Exception as e:
#         logging.info(e)
