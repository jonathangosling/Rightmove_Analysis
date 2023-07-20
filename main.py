from datetime import date, datetime
import ETL.scrape as scrape
import ETL.financials as financials
import ETL.load as load
import logging

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
scrape.transform(data, Logger)

Logger.info("Connecting to database to find the latest date of interest rate data ...")
conn = load.connect_to_db()
cursor = conn.cursor()
query = '''select MAX(date) from dbo.interest_rates'''
cursor.execute(query)
results = cursor.fetchall()
cursor.close()
previous_date = datetime.strptime(results[0][0], "%Y-%m-%d")
Logger.info("Got the latest date of interest rate data and converted to string.")

Logger.info("Zipping property data to tuples ...")
property_tuples = data.zip_property()
Logger.info("Zipping price data to tuples ...")
price_tuples = data.zip_price(date_today=date_today)

interest_rate_tuples = financials.get_interest_rates(previous_date, date_today, Logger)
SPY_tuple = financials.get_spy_price(Logger)

Logger.info("Connecting to database to start loading the data ...")
conn = load.connect_to_db()

property_query = '''
                WITH source AS (
                SELECT * FROM (VALUES (?, ?, ?, ?, ?)) s(prop_id, address, postcode, latitude, longitude)
                )
                INSERT INTO dbo.property
                (prop_id, address, postcode, latitude, longitude)
                SELECT source.prop_id, source.address, source.postcode, 
                source.latitude, source.longitude 
                FROM source
                WHERE NOT EXISTS
                (SELECT 1 FROM dbo.property target
                WHERE target.prop_id = source.prop_id)
                '''

Logger.info("Loading into property table ...")
initial_size = load.get_table_size(conn, "dbo.property")
load.load_data(conn, property_query, property_tuples)
final_size = load.get_table_size(conn, "dbo.property")
Logger.info(f"{final_size-initial_size} items loaded.")

price_query = '''
                WITH source AS (
                SELECT * FROM (VALUES (?, ?, ?)) s(prop_id, date, price)
                )
                INSERT INTO dbo.price
                (prop_id, date, price)
                SELECT source.prop_id, source.date, source.price 
                FROM source
                WHERE NOT EXISTS
                (SELECT 1 FROM dbo.price target
                WHERE target.prop_id = source.prop_id
                AND target.date = source.date)
                '''

Logger.info("Loading into price table ...")
initial_size = load.get_table_size(conn, "dbo.price")
load.load_data(conn, price_query, price_tuples)
final_size = load.get_table_size(conn, "dbo.price")
Logger.info(f"{final_size-initial_size} items loaded.")

interest_rate_query = '''
                        WITH source AS (
                        SELECT * FROM (VALUES (?, ?)) s(date, rate)
                        )
                        INSERT INTO dbo.interest_rates
                        (date, IUMSOIA)
                        SELECT source.date, source.rate
                        FROM source
                        WHERE NOT EXISTS
                        (SELECT 1 FROM dbo.interest_rates target
                        WHERE target.date = source.date
                        )
                        '''

Logger.info("Loading into interest_rates table ...")
initial_size = load.get_table_size(conn, "dbo.interest_rates")
load.load_data(conn, interest_rate_query, interest_rate_tuples)
final_size = load.get_table_size(conn, "dbo.interest_rates")
Logger.info(f"{final_size-initial_size} items loaded.")

SPY_query = '''
            WITH source AS (
            SELECT * FROM (VALUES (?, ?)) s(date, [close])
            )
            INSERT INTO dbo.SPY_price
            (date, [close])
            SELECT source.date, source.[close]
            FROM source
            WHERE NOT EXISTS
            (SELECT 1 FROM dbo.SPY_price target
            WHERE target.date = source.date
            )
            '''

Logger.info("Loading into SPY_price table ...")
initial_size = load.get_table_size(conn, "dbo.SPY_price")
load.load_data(conn, SPY_query, SPY_tuple)
final_size = load.get_table_size(conn, "dbo.SPY_price")
Logger.info(f"{final_size-initial_size} items loaded.")

conn.close()
Logger.info("Closed connection to database.")
