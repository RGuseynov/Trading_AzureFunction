# Trading_AzureFunction

The Azure Function performs following steps every hour: 
1. Retrieve last BTC price from an API
2. Insert this last price in a database
3. Retrieve last 200 prices from the database (needed for inference)
4. Make prediction
5. Insert prediction in a database