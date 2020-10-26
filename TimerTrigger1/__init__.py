import os
import datetime
import logging

import requests
import pyodbc

import azure.functions as func

# from azure.identity import DefaultAzureCredential
# from azure.identity import ManagedIdentityCredential
# from azure.keyvault.secrets import SecretClient


def main(mytimer: func.TimerRequest) -> None:
    logging.info('enter function')
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)

    password = os.environ['SQL_SERVER_CONNECTION']

    # credential = DefaultAzureCredential()
    # credential = ManagedIdentityCredential()

    # secret_client = SecretClient(vault_url="https://tradingkeyvault.vault.azure.net/", credential=credential)
    # secret = secret_client.get_secret("sqlserver-trading")

    # logging.info(f"secret name is: {secret.name}")

    cnxn = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};Server=tcp:sqlserver-trading.database.windows.net,1433;Database=financial;Uid=MasterTrader;Pwd="+password+";Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;")
    cursor = cnxn.cursor()

    logging.info("connection done")

    cursor.execute("SELECT * FROM {}".format('SP500')) 
    # row = cursor.fetchone()
    rows = cursor.fetchall()

    logging.info("row fetched")

    logging.info(rows)

