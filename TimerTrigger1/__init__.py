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


def main(mytimer: func.TimerRequest) -> None:

    logging.getLogger().setLevel(logging.INFO)

    logging.info('enter function')
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.warning('The timer is past due!')
    logging.info('Python timer trigger function ran at %s', utc_timestamp)


    try:
        coinAPI_key = os.environ['COIN_API2']
        database_password = os.environ['SQL_SERVER_CONNECTION']
    except Exception as e:
        logging.error(e)

    try:
        url = 'https://rest.coinapi.io/v1/ohlcv/BITSTAMP_SPOT_BTC_USD/latest?period_id=1HRS'
        headers = {'X-CoinAPI-Key' : coinAPI_key}
        response = requests.get(url, headers=headers)
        data = response.json()
    except Exception as e:
        logging.error(e)

    datetime_now = datetime.datetime.utcnow()
    datetime_now = datetime_now.replace(microsecond=0, second=0, minute=0)
    actual_date_stamp = None

    logging.info("getting the datetime")

    for row in data[:3]:
        row_periode_end = datetime.datetime.strptime(row["time_period_end"], '%Y-%m-%dT%H:%M:%S.%f0000Z')
        if(datetime_now == row_periode_end):
            logging.info("equal datetime found")
            actual_date_stamp = row
    last = actual_date_stamp

    try:
        cnxn = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};Server=tcp:sqlserver-trading.database.windows.net,1433;Database=financial;Uid=MasterTrader;Pwd="+database_password+";Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;")
        cursor = cnxn.cursor()
        logging.info("connection to SQL Server done")
    except Exception as e:
        logging.error(e)

    try:
        cursor.execute("INSERT INTO bitcoin_1H_bitstampUSD_latest VALUES (?, ?, ?, ?, ?, ?)", 
                last["time_period_start"][:-5], last["price_open"], last["price_high"], 
                last["price_low"], last["price_close"], last["volume_traded"])
        cnxn.commit()
        logging.info("price insert done")
    except Exception as e:
        logging.info(e)


    sql_query = ("SELECT TOP(200) * FROM bitcoin_1H_bitstampUSD_latest ORDER BY Timestamp DESC")
    df = pd.read_sql(sql_query, cnxn)
    logging.info("select request done")
    logging.info(df)

    try:
        df = df.iloc[::-1].reset_index()
        df = df.drop(['Timestamp', 'index'], axis=1)
    except Exception as e:       
        logging.error(e)

    fe.add_TA(df)
    logging.info("TA_added")
    logging.info(df)
    last_row = df.tail(1)
    logging.info("last_row_retrieved")

    try:
        with open("TimerTrigger1/xgb-BSH-training-stocks.pkl", "rb") as file:
            xgb_model = pickle.load(file)      
        logging.info("model loaded")
    except Exception as e:
        logging.error(e)

    try:
        last_row = xgb.DMatrix(last_row)
        logging.info("DMatrix created")
    except Exception as e:
        logging.error(e)

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
        logging.error(e)



#CODE BELOW TO EXECUTE INITIALY OR IF AZURE FUNCTION STOP WORKING IN ORDER TO REFRESH MISSING DATA

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
#                 last["time_period_start"][:-5], last["price_open"], last["price_high"], last["price_low"],
#                 last["price_close"], last["volume_traded"])
#         cnxn.commit()
#     except Exception as e:
#         print(e)




# datetime_now = datetime.datetime.utcnow()
# datetime_now = datetime_now.replace(microsecond=0, second=0, minute=0)
# actual_date_stamp = None

# for row in data[:3]:
#     row_periode_end = datetime.datetime.strptime(row["time_period_end"], '%Y-%m-%dT%H:%M:%S.%f0000Z')
#     if(datetime_now == row_period_end["time_period_end"]):
#         print("equal")
#         actual_date_stamp = row
# row