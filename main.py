from datetime import date, datetime, timedelta
import ETL.scrape as scrape
import ETL.financials as financials
import ETL.load as load
import logging

def ETL_to_database(ODBC_Driver = "SQL Server", schema = 'dbo'):

    logging.basicConfig(
        filename="log_file.txt",
        filemode="w", # here the value is 'w' for write, but the default is 'a' for append
        level=logging.INFO, # set our minumum level
        format=r'%(levelname)-10s --- %(asctime)s: %(message)s',
        datefmt='%Y/%m/%d %H:%M:%S'
    )
    Logger = logging.getLogger()

    date_today = date.today()
    url = r'''https://www.rightmove.co.uk/property-to-rent/find.html?locationIdentifier=REGION%5E92829&maxBedrooms=2&minBedrooms=2&propertyTypes=&includeLetAgreed=false&mustHave=&dontShow=&furnishTypes=&keywords='''

    data = scrape.scrape(url, Logger)
    print(data.prices)
    scrape.transform(data, Logger)

    Logger.info("Connecting to database to find the latest date of interest rate data ...")
    conn = load.connect_to_db(ODBC_Driver, database = 'properties')
    cursor = conn.cursor()
    query = f'''select MAX(date) from {schema}.interest_rates'''
    cursor.execute(query)
    results = cursor.fetchall()
    previous_date = results[0][0]
    cursor.close()
    if previous_date:
        if isinstance(previous_date, str): 
            previous_date = datetime.strptime(previous_date, "%Y-%m-%d")
    else:
        # if there is no previous date in the database 
        # (i.e. if we've not yet loaded any interest rate data)
        # then let's look over the last 7 days
        previous_date = date_today - timedelta(days=7)
    Logger.info("Got the latest date of interest rate data.")

    Logger.info("Zipping property data to tuples ...")
    property_tuples = data.zip_property()
    Logger.info("Zipping price data to tuples ...")
    price_tuples = data.zip_price(date_today=date_today)

    interest_rate_tuples = financials.get_interest_rates(previous_date, date_today, Logger)
    SPY_tuple = financials.get_spy_price(Logger)

    Logger.info("Connecting to database to start loading the data ...")
    conn = load.connect_to_db(ODBC_Driver, database='properties')

    property_query = f'''
                    WITH source AS (
                    SELECT * FROM (VALUES (?, ?, ?, ?, ?)) s(prop_id, address, postcode, latitude, longitude)
                    )
                    INSERT INTO {schema}.property
                    (prop_id, address, postcode, latitude, longitude)
                    SELECT source.prop_id, source.address, source.postcode, 
                    source.latitude, source.longitude 
                    FROM source
                    WHERE NOT EXISTS
                    (SELECT 1 FROM {schema}.property target
                    WHERE target.prop_id = source.prop_id)
                    '''

    Logger.info("Loading into property table ...")
    initial_size = load.get_table_size(conn, schema+".property")
    load.load_data(conn, property_query, property_tuples)
    final_size = load.get_table_size(conn, schema+".property")
    Logger.info(f"{final_size-initial_size} items loaded.")

    price_query = f'''
                    WITH source AS (
                    SELECT * FROM (VALUES (?, ?, ?)) s(prop_id, date, price)
                    )
                    INSERT INTO {schema}.price
                    (prop_id, date, price)
                    SELECT source.prop_id, source.date, source.price 
                    FROM source
                    WHERE NOT EXISTS
                    (SELECT 1 FROM {schema}.price target
                    WHERE target.prop_id = source.prop_id
                    AND target.date = source.date)
                    '''

    Logger.info("Loading into price table ...")
    initial_size = load.get_table_size(conn, schema+".price")
    load.load_data(conn, price_query, price_tuples)
    final_size = load.get_table_size(conn, schema+".price")
    Logger.info(f"{final_size-initial_size} items loaded.")

    interest_rate_query = f'''
                            WITH source AS (
                            SELECT * FROM (VALUES (?, ?)) s(date, rate)
                            )
                            INSERT INTO {schema}.interest_rates
                            (date, IUMSOIA)
                            SELECT source.date, source.rate
                            FROM source
                            WHERE NOT EXISTS
                            (SELECT 1 FROM {schema}.interest_rates target
                            WHERE target.date = source.date
                            )
                            '''

    Logger.info("Loading into interest_rates table ...")
    initial_size = load.get_table_size(conn, schema+".interest_rates")
    load.load_data(conn, interest_rate_query, interest_rate_tuples)
    final_size = load.get_table_size(conn, schema+".interest_rates")
    Logger.info(f"{final_size-initial_size} items loaded.")

    SPY_query = f'''
                WITH source AS (
                SELECT * FROM (VALUES (?, ?)) s(date, [close])
                )
                INSERT INTO {schema}.SPY_price
                (date, [close])
                SELECT source.date, source.[close]
                FROM source
                WHERE NOT EXISTS
                (SELECT 1 FROM {schema}.SPY_price target
                WHERE target.date = source.date
                )
                '''

    Logger.info("Loading into SPY_price table ...")
    initial_size = load.get_table_size(conn, schema+".SPY_price")
    load.load_data(conn, SPY_query, SPY_tuple)
    final_size = load.get_table_size(conn, schema+".SPY_price")
    Logger.info(f"{final_size-initial_size} items loaded.")

    conn.close()
    Logger.info("Closed connection to database.")

def ETL_to_mart(ODBC_Driver = 'SQL Server', schema = 'dbo'):
    conn = load.connect_to_db(ODBC_Driver)
    cursor = conn.cursor()
    cursor.execute(f"EXEC property_mart.{schema}.ETL_to_mart")
    cursor.commit()
    cursor.close()

if __name__ == '__main__':
    ETL_to_database()
